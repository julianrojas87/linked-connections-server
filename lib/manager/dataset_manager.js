const util = require('util');
const fs = require('fs');
const del = require('del');
const child_process = require('child_process');
const cron = require('cron');
const url = require('url');
const http = require('follow-redirects').http;
const https = require('follow-redirects').https;
const Logger = require('../utils/logger');
const utils = require('../utils/utils');
const { Connections, Connections2JSONLD } = require('gtfs2lc');
const jsonldstream = require('jsonld-stream');
const pageWriterStream = require('./pageWriterStream.js');
const { GtfsIndex, Gtfsrt2LC } = require('gtfsrt2lc');
const pathOperations = require('path');
const EventManager = require('./events_manager.js');

const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
var logger = null;

class DatasetManager {
    constructor() {
        this._config = utils.datasetsConfig;
        this._datasets = this._config.datasets;
        this._storage = this._config.storage;
        this._jobs = [];
        this._indexes = [];
        this._EventsManager = new EventManager();
        logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');
        this.initDirs();
    }

    initDirs() {
        if (this.storage.endsWith('/')) {
            this.storage = this.storage.substring(0, this.storage.length - 1);
        }

        if (!fs.existsSync(this.storage)) {
            fs.mkdirSync(this.storage);
        }

        if (!fs.existsSync(this.storage + '/tmp')) {
            fs.mkdirSync(this.storage + '/tmp');
        }

        if (!fs.existsSync(this.storage + '/datasets')) {
            fs.mkdirSync(this.storage + '/datasets');
        }

        if (!fs.existsSync(this.storage + '/linked_connections')) {
            fs.mkdirSync(this.storage + '/linked_connections');
        }

        if (!fs.existsSync(this.storage + '/linked_pages')) {
            fs.mkdirSync(this.storage + '/linked_pages');
        }

        if (!fs.existsSync(this.storage + '/real_time')) {
            fs.mkdirSync(this.storage + '/real_time');
        }

        if (!fs.existsSync(this.storage + '/vehicles')) {
            fs.mkdirSync(this.storage + '/vehicles');
        }

        if (!fs.existsSync(this.storage + '/events')) {
            fs.mkdirSync(this.storage + '/events');
        }
    }

    async manage() {
        // Verify that there are no incomplete processes
        await this.cleanUpIncompletes();

        this._datasets.forEach(async (dataset, index) => {
            try {
                // Create necessary dirs
                this.initCompanyDirs(dataset.companyName);
                // Schedule GTFS feed processing job
                this.launchStaticJob(index, dataset);

                // Download and process GTFS feed on server launch if required
                if (dataset.downloadOnLaunch) {
                    this.processStaticGTFS(index, dataset);
                }

                // Handle real-time data if available
                if (dataset.realTimeData) {
                    // Get GTFS indexes
                    let loaded = await this.loadGTFSIdentifiers(index, dataset);
                    // If identifiers were loaded schedule GTFS-RT processing job
                    if (loaded) {
                        this.launchRTJob(index, dataset);
                        // Schedule compression or RT files
                        this.rtCompressionJob(dataset);
                    } else {
                        logger.warn('There are no GTFS datasets present for ' + dataset.companyName +
                            '. Make sure to provide one before handling real-time data.');
                    }
                }
            } catch (err) {
                logger.error(err);
            }
        });
    }

    initCompanyDirs(name) {
        if (!fs.existsSync(this.storage + '/datasets/' + name)) {
            fs.mkdirSync(this.storage + '/datasets/' + name);
        }

        if (!fs.existsSync(this.storage + '/linked_connections/' + name)) {
            fs.mkdirSync(this.storage + '/linked_connections/' + name);
        }

        if (!fs.existsSync(this.storage + '/linked_pages/' + name)) {
            fs.mkdirSync(this.storage + '/linked_pages/' + name);
        }
    }

    launchStaticJob(index, dataset) {
        let static_job = new cron.CronJob({
            cronTime: dataset.updatePeriod,
            onTick: () => {
                this.processStaticGTFS(index, dataset);
            },
            start: true
        });

        if (!this.jobs[index]) this.jobs[index] = {};
        this.jobs[index]['static_job'] = static_job;
        logger.info('GTFS job for ' + dataset.companyName + ' scheduled correctly');
    }

    async processStaticGTFS(index, dataset) {
        let t0 = new Date().getTime();
        let t1;
        let companyName = dataset.companyName;
        logger.info('Running cron job to update ' + companyName + ' GTFS feed');
        try {
            // Download GTFS feed
            let file_name = await this.downloadDataset(dataset);
            if (file_name != null) {
                // Create .lock file to prevent incomplete transformations
                writeFile(this.storage + '/datasets/' + companyName + '/' + file_name + '.lock', file_name
                    + ' GTFS feed being transformed to Linked Connections');

                // Reload GTFS identifiers and static indexes for RT processing, using new GTFS feed files
                if (dataset.realTimeData) {
                    logger.info('Updating GTFS identifiers for ' + companyName + '...');
                    // First pause RT job if is already running
                    if (this.jobs[index]['rt_job']) {
                        this.jobs[index]['rt_job'].stop();
                    }

                    this.loadGTFSIdentifiers(index, dataset).then(() => {
                        // Start RT job again or create new one if does not exist
                        if (this.jobs[index]['rt_job']) {
                            this.jobs[index]['rt_job'].start();
                        } else {
                            this.launchRTJob(index, dataset);
                        }
                    });
                }

                let path = this.storage + '/datasets/' + companyName + '/' + file_name + '.zip';
                // Unzip it
                let uncompressed_feed = await utils.readAndUnzip(path);
                logger.info(companyName + ' Dataset uncompressed');
                // Get base URIs for conversion to Linked Connections
                let baseURIs = this.getBaseURIs(dataset);
                // Organize GTFS data according to the required order by gtfs2lc tool 
                await this.preSortGTFS(uncompressed_feed);
                // Convert to Linked Connections
                let converter = new Connections({ store: 'MemStore' });
                // Stream into a file for sorting
                let fileWriter = fs.createWriteStream(this.storage + '/linked_connections/' + companyName + '/' + file_name + '_tmp.jsonld', 'utf8');

                converter.resultStream(uncompressed_feed, resultStream => {
                    logger.info('Creating ' + companyName + ' Linked Connections...');

                    resultStream.pipe(new Connections2JSONLD(baseURIs))
                        .pipe(new jsonldstream.Serializer())
                        .pipe(fileWriter)
                        .on('finish', async () => {
                            // Delete uncompressed GTFS data
                            del([this.storage + '/datasets/' + companyName + '/' + file_name + '_tmp'], { force: true });
                            logger.info("Sorting " + companyName + " Linked Connections by departure time...");
                            let sorted_path = await this.sortLCByDepartureTime(this.storage + '/linked_connections/' + companyName + '/'
                                + file_name + '_tmp.jsonld', companyName, file_name);
                            logger.info('Fragmenting ' + companyName + ' Linked Connections...');
                            // Delete unsorted Linked Connections graph
                            del([this.storage + '/linked_connections/' + companyName + '/' + file_name + '_tmp.jsonld'], { force: true });
                            // Create folder for Linked Connection fragments
                            fs.mkdirSync(this.storage + '/linked_pages/' + companyName + '/' + file_name)
                            // Proceed to fragment the Linked Connections graph
                            let reader = fs.createReadStream(sorted_path, 'utf8');
                            reader.pipe(new jsonldstream.Deserializer())
                                .pipe(new pageWriterStream(this.storage + '/linked_pages/' + companyName + '/'
                                    + file_name, dataset.fragmentSize || 50000))
                                .on('finish', async () => {
                                    logger.info('Compressing ' + companyName + ' Linked Connections fragments...');
                                    child_process.spawn('gzip', [file_name + '.jsonld'], {
                                        cwd: this.storage + '/linked_connections/' + companyName,
                                        stdio: 'ignore'
                                    });
                                    let comp = child_process.spawn('find', ['.', '-type', 'f', '-exec', 'gzip', '{}', '+'], {
                                        cwd: this.storage + '/linked_pages/' + companyName + '/' + file_name,
                                        stdio: 'ignore'
                                    });
                                    comp.on('close', async (code) => {
                                        t1 = (new Date().getTime() - t0) / 1000;
                                        logger.info('Dataset conversion for ' + companyName + ' completed successfuly (took ' + t1 + ' seconds)');
                                        // GTFS feed completed successfuly, proceed to delete .lock file
                                        del([this.storage + '/datasets/' + companyName + '/' + file_name + '.lock'], { force: true });

                                        // Generate vehicle index
                                        await this.sortPagesByTripURI(dataset);

                                        // TEST cancel trip
                                        //this.cancelTrip('http://irail.be/vehicle/IC106/20190226', companyName, dataset);
                                    });
                                });
                        });
                });
            } else {
                logger.warn(companyName + " dataset was already downloaded");
            }
        } catch (err) {
            logger.error(err);
        }
    }

    async loadGTFSIdentifiers(index, dataset) {
        try {
            let datasets_dir = this.storage + '/datasets/' + dataset.companyName;
            // Delete any _tmp folders (this means something went wrong last time)
            await del([datasets_dir + '/*_tmp', datasets_dir + '/.tmp'], { force: true });

            let lastGtfs = await utils.getLatestGtfsSource(dataset.companyName);

            if (lastGtfs !== null) {
                let indexer = new GtfsIndex(lastGtfs);
                let [routes, trips] = await indexer.getIndexes();
                // Store in memory the indexes for this company
                if (!this.indexes[index]) this.indexes[index] = [];
                this.indexes[index]['routes'] = routes;
                this.indexes[index]['trips'] = trips;

                return true;
            }
        } catch (err) {
            logger.error(err);
            return false;
        }

        return false;
    }

    launchRTJob(index, dataset) {
        let companyName = dataset.companyName;
        let rt_path = this.storage + '/real_time/' + companyName;
        if (!fs.existsSync(rt_path)) {
            fs.mkdirSync(rt_path);
        }

        // Object to keep in memory the .remove files and avoid reading them from the disk every time.
        let removeCache = {};

        let rt_job = new cron.CronJob({
            cronTime: dataset.realTimeData.updatePeriod,
            onTick: async () => {
                try {
                    // Timestamp that indicates when the data was obtained
                    let timestamp = new Date();
                    // Array to store all the incoming raw connection updates
                    let rawData = [];
                    // Object to group the updates by fragment 
                    let rtDataObject = {};
                    // Object to keep track of the connections that are moved to different fragments due to delays
                    let removeList = {};
                    // Group all connection updates into fragment based arrays according to predefined fragment time span
                    let fragTimeSpan = dataset.realTimeData.fragmentTimeSpan || 600;
                    // Proceed to parse GTFS-RT
                    let parser = new Gtfsrt2LC(dataset.realTimeData.downloadUrl, this.indexes[index]['routes'], this.indexes[index]['trips'],
                        dataset.baseURIs);
                    // Remove old remove records that won't be used anymore
                    removeCache = this.cleanRemoveCache(removeCache, timestamp);
                    // Use JSON-LD as output format
                    let rtlc = await parser.parse('jsonld', true);

                    rtlc.on('data', data => {
                        // Ignore @context
                        if (!data['@context']) {
                            rawData.push(data);
                        }
                    });

                    rtlc.on('end', async () => {
                        await Promise.all(rawData.map(async jodata => {
                            // Determine current fragment that the connection belongs to, due to delays
                            let ndt = new Date(jodata.departureTime);
                            let newFragment = new Date(ndt.getTime() - (ndt.getTime() % (fragTimeSpan * 1000)));
                            // Determine connection's original fragment
                            let odt = new Date(ndt.getTime() - (jodata['departureDelay'] * 1000));
                            let origFragment = new Date(odt.getTime() - (odt.getTime() % (fragTimeSpan * 1000)));

                            // Check if there are previous remove reports for the original fragment
                            if (fs.existsSync(rt_path + '/' + origFragment.toISOString() + '.remove')) {
                                let origRemove = null;
                                let remRecord = null;

                                // Check if the .remove file has been previously read from the disk
                                if (removeCache[origFragment.toISOString()]) {
                                    origRemove = removeCache[origFragment.toISOString()];
                                } else {
                                    let removeFile = (await readFile(rt_path + '/' + origFragment.toISOString() + '.remove', 'utf8')).split('\n');
                                    removeFile.pop();
                                    origRemove = new Map();

                                    for (let k in removeFile) {
                                        let rec = removeFile[k].split(',');
                                        origRemove.set(rec[0], rec);
                                    }
                                    // Keep in memory in case it needs to be reused
                                    removeCache[origFragment.toISOString()] = origRemove;
                                }
                                // Look for the last record of removal for this Connection, if any.
                                if (origRemove.has(jodata['@id'])) {
                                    remRecord = origRemove.get(jodata['@id']);
                                }

                                // Previous record found
                                if (remRecord != null) {
                                    // Get the previous fragments where the connection has been
                                    remRecord = remRecord.slice(2);
                                    // Register the removal record on other fragments where the connection has been
                                    for (let j in remRecord) {
                                        // Skip the current fragment because it is where the connection is now
                                        if (remRecord[j] != newFragment.toISOString()) {
                                            if (!removeList[remRecord[j]]) {
                                                removeList[remRecord[j]] = [{ '@id': jodata['@id'] }];
                                            } else {
                                                removeList[remRecord[j]].push({ '@id': jodata['@id'] });
                                            }
                                            // Update the removeCache to avoid reloading the file afterwards
                                            if(removeCache[remRecord[j]]) {
                                                removeCache[remRecord[j]].set(jodata['@id'], [jodata['@id'], timestamp.toISOString()]);
                                            }
                                        }
                                    }

                                    // Add new fragment to the connection record if it is being written in a new fragment
                                    if (origFragment.getTime() != newFragment.getTime()) {
                                        if (remRecord.indexOf(newFragment.toISOString()) < 0) {
                                            remRecord.push(newFragment.toISOString());
                                        }
                                    }
                                } else {
                                    // There was no previous record, create it if it is being written in a new fragment
                                    if (origFragment.getTime() != newFragment.getTime()) {
                                        remRecord = [newFragment.toISOString()];
                                    }
                                }

                                // Register the removal record on the original fragment while keeping record of all the fragments
                                // the connection has been.
                                if (origFragment.getTime() != newFragment.getTime()) {
                                    if (!removeList[origFragment.toISOString()]) {
                                        removeList[origFragment.toISOString()] = [{ '@id': jodata['@id'], 'track': remRecord }];
                                    } else {
                                        removeList[origFragment.toISOString()].push({ '@id': jodata['@id'], 'track': remRecord });
                                    }

                                    // Update the removeCache to avoid reloading the file afterwards
                                    let newRec = [jodata['@id'], timestamp.toISOString()];
                                    if(remRecord != null) {
                                        newRec = newRec.concat(remRecord);
                                    }
                                    
                                    removeCache[origFragment.toISOString()].set(jodata['@id'], newRec);
                                    
                                }
                            }
                            else {
                                // There is no .remove file for the original file yet. Just register the remove record on the original 
                                // fragment and keep track of the new fragment the connection is moving to.
                                if (origFragment.getTime() != newFragment.getTime()) {
                                    if (!removeList[origFragment.toISOString()]) {
                                        removeList[origFragment.toISOString()] = [{ '@id': jodata['@id'], 'track': [newFragment.toISOString()] }];
                                    } else {
                                        removeList[origFragment.toISOString()].push({ '@id': jodata['@id'], 'track': [newFragment.toISOString()] });
                                    }
                                }
                            }

                            // TEST EVENTS, add event
                            //console.log('Adding events to EventsManager:' + JSON.stringify(jodata, null, 4) + '\n\n');
                            this.EventsManager.addEvent(jodata, companyName);

                            // Add timestamp to RT data for versioning
                            jodata['mementoVersion'] = timestamp.toISOString();
                            if (!rtDataObject[newFragment.toISOString()]) rtDataObject[newFragment.toISOString()] = [];
                            rtDataObject[newFragment.toISOString()].push(JSON.stringify(jodata));
                        }));
                        // Write new data into fragment files
                        await this.updateRTData(rtDataObject, rt_path);
                        // Write removeList into file
                        this.storeRemoveList(removeList, rt_path, timestamp.toISOString());

                        // All GTFS-RT events are ready, process generated events
                        this.EventsManager.processEvents(timestamp);

                        let t1 = new Date().getTime();
                        let tf = t1 - timestamp.getTime();
                        logger.info(companyName + ' GTFS-RT feed updated for version ' + timestamp.toISOString() + ' (took ' + tf + ' ms)');
                    });
                } catch (err) {
                    logger.error('Error getting GTFS-RT feed for ' + companyName + ': ' + err);
                }
            },
            start: true
        });

        // Store Job reference for management
        if (!this.jobs[index]) this.jobs[index] = {};
        this.jobs[index]['rt_job'] = rt_job;
        logger.info('GTFS-RT job for ' + companyName + ' scheduled correctly');
    }

    rtCompressionJob(dataset) {
        let companyName = dataset.companyName;

        let rt_compression_job = new cron.CronJob({
            cronTime: dataset.realTimeData.compressionPeriod,
            onTick: async () => {
                try {
                    let path = this.storage + '/real_time/' + companyName + '/';
                    let now = new Date();

                    let fgmts = await readdir(path);

                    fgmts.forEach(fg => {
                        if (fg.indexOf('.gz') < 0) {
                            // Get last modification time of the file
                            let stats = fs.statSync(path + '/' + fg);
                            let lastModified = new Date(util.inspect(stats.mtime));

                            // If the file hasn't been modified in the last 4 hours, compress it
                            if (now.getTime() - lastModified.getTime() >= 14400000) {
                                child_process.spawn('gzip', [fg], { cwd: path, detached: true });
                            }
                        }
                    });

                } catch (err) {
                    logger.error('Error compressing RT files for ' + companyName + ': ' + err);
                }
            },
            start: true
        });
    }

    /**
     * @brief Sorts the static pages by the trip URI of a vehicle.
     * The sorting is based on the trip URI of a vehicle, the folder structure is the same as the structure from the
     * trip URI. Looking up a trip only requires to open a file with the trip URI as path and the right file. In this
     * case the file is called: `pages.json`.
     * @author Dylan Van Assche
     */
    async sortPagesByTripURI(dataset) {
        return new Promise(async (resolve, reject) => {
            logger.info('Updating vehicle index...');
            let t0 = new Date().getTime();
            let t1;

            let companyName = dataset.companyName;
            let vehiclePath = this.storage + '/vehicles/' + companyName;
            let pagesPath = this.storage + '/linked_pages/' + companyName;
            let currentTrip = {};

            // Check if folders exists for the dataset and create them if needed.
            if (!fs.existsSync(vehiclePath)) {
                fs.mkdirSync(vehiclePath);
            }

            if(fs.existsSync(vehiclePath + '/sorting.lock')) {
                logger.error('Incomplete sorting on tripURI found for data sets, cleaning up...');
                // Remove old files aka: `rm -r {dataFolder}/vehicles/*`
                await del([vehiclePath + '/*'], { force: true });
                logger.error('Cleaned incomplete tripURI sorting for data sets');
            }

            await writeFile(vehiclePath + '/sorting.lock', 'Sorting data sets on tripURI');

            fs.readdir(pagesPath, async (err, sets) => {
                logger.debug('Found: ' + sets.length + ' data sets');

                // Loop through each linked_page entry
                for (let i = 0; i < sets.length; i++) {
                    fs.readdir(pagesPath + '/' + sets[i], async (err, pages) => {
                        logger.debug(pages.length + ' pages in data set');

                        // Loop through each page of each linked_page entry
                        for (let j = 0; j < pages.length; j++) {
                            // Unzip each page and parse the trip URIs of each connection
                            let uncompressedFeed = await utils.readAndGunzip(pagesPath + '/' + sets[i] + '/' + pages[j]);
                            let graph = uncompressedFeed.split(',\n');
                            let jsonldGraph = graph.map(JSON.parse);

                            for (let k = 0; k < jsonldGraph.length; k++) {
                                let tripURI = jsonldGraph[k]['gtfs:trip'];
                                let tripPath = vehiclePath + '/' + tripURI;
                                process.stdout.write('Processing connection ' + tripURI.padEnd(50) + ' of page ' + j + '/' + pages.length + ' in data set ' + sets[i] + '\r'); // padEnd for formatting string
                                
                                // Create the path if needed
                                try {
                                    // Requires NodeJS >= 10.X.X
                                    fs.mkdirSync(tripPath, {
                                        recursive: true
                                    });
                                }
                                catch(err) {
                                    // Ignore exist errors
                                    if (err.code !== 'EEXIST') {
                                        throw err;
                                    }
                                }
                                
                                // If a `pages.json` exists for this trip URI, read the current list of page URIs
                                let hasCurrentTrip = false;
                                if(fs.existsSync(tripPath + '/pages.json')) {
                                    let d = await readFile(tripPath + '/pages.json', 'utf8');
                                    currentTrip = JSON.parse(d);
                                    hasCurrentTrip = true;
                                }
                                
                                // New trip, no file exists yet
                                let currentPage = pathOperations.basename(pages[j], '.jsonld.gz')
                                if(!hasCurrentTrip) {
                                    currentTrip = {
                                        'departurePageTime': currentPage,
                                        'arrivalPageTime': currentPage
                                    }
                                    await writeFile(tripPath + '/pages.json', JSON.stringify(currentTrip));
                                }
                                // Avoid to add double page URI entries
                                else if(currentTrip['arrivalPageTime'] !== currentPage && currentTrip['departurePageTime'] !== currentPage) {
                                    // Add new page URI to the list and write it to disk
                                    let currentDepartureTime = new Date(currentTrip['departurePageTime']);
                                    let currentArrivalTime = new Date(currentTrip['arrivalPageTime']);
                                    let newPageTime = new Date(currentPage);
                                    
                                    currentTrip = {
                                        'departurePageTime': currentTrip['departurePageTime'],
                                        'arrivalPageTime': currentTrip['arrivalPageTime']
                                    };

                                    if(newPageTime.getTime() < currentDepartureTime.getTime()) {
                                        currentTrip['departurePageTime'] = currentPage;
                                    }
                                    
                                    if(newPageTime.getTime() > currentArrivalTime.getTime()) {
                                        currentTrip['arrivalPageTime'] = currentPage;
                                    }
                                    
                                    await writeFile(tripPath + '/pages.json', JSON.stringify(currentTrip));
                                }
                            }
                        }

                        // Finished looping through pages, resolve Promise and show benchmark
                        if(i == sets.length - 1) {
                            t1 = (new Date().getTime() - t0) / 1000;
                            logger.info('Vehicle index updated successfully (took ' + t1 + ' seconds)');
                            resolve();
                        }
                    });
                }
            });
        });
    }

    /**
     * Cancel a complete trip based on tripURI and starting connection.
     * The starting connection is the first connection from where the vehicle is cancelled.
     * If the vehicle is completely cancelled, you can omit this parameter..
     *
     * Example: `await this.cancelTrip('http://irail.be/vehicle/IC106/20190301', 'sncb');`
     * @param tripURI
     * @param companyName
     * @param dataset
     * @param startingConnection = null
     * @return `true` if successful
     */
    async cancelTrip(tripURI, companyName, dataset, startingConnection = null) {
        logger.debug('Cancelling trip: ' + tripURI);
        let vehiclePath = this.storage + '/vehicles/' + companyName;
        let pagesPath = this.storage + '/linked_pages/' + companyName;
        let rtPath = this.storage + '/real_time/' + companyName;

        // Get trip data and check if we have to start at the beginning of the trip or not
        let tripData;
        try {
            tripData = JSON.parse(await readFile(vehiclePath + '/' + tripURI + '/pages.json'));
            console.debug(tripData);
        }
        catch(err) {
            if(err.code == 'ENOENT') {
                logger.error('Unable to find the trip data for this trip URI: ' + tripURI + ', is the provided URI a valid trip URI?');
            }
            else {
                logger.error('Unknown error when looking up the trip data for trip URI: ' + tripURI);
            }
            return false;
        }
        let startingAt = tripData['departurePageTime'];
        if(startingConnection != null) {
            startingAt = startingConnection;
        }

        // Find the pages where the connections of the vehicle are saved
        let arrivalPageTime = new Date(tripData['arrivalPageTime']);
        let currentPageTime = new Date(startingAt);

        // Memento support: current version of the fragments is requested
        await utils.updateStaticFragments(); // Populate the staticFragments object for the first time
        let versions = Object.keys(utils.staticFragments[companyName]);
        let now = new Date();

        // Loop through all the pages until we reach the arrival connection of the vehicle (which is found in the arrivalPageTime fragment)
        logger.debug('Starting to search at: ' + currentPageTime);
        let foundCancelledConnection = null;
        let currentRTFragment = null;
        let currentRTFragmentDate = null;
        let nextRTFragment = null;
        let nextRTFragmentDate = null;
        let rtFragmentsList = await readdir(rtPath);
        rtFragmentsList = rtFragmentsList.filter((value) => { return pathOperations.extname(value) === '.jsonld'});
        rtFragmentsList.sort();
        console.log(rtFragmentsList);
        while(arrivalPageTime.getTime() >= currentPageTime) {
            let sorted_versions = utils.sortVersions(now, versions);
            let [static_version, found_fragment, index] = utils.findResource(companyName, currentPageTime.getTime(), sorted_versions);
            let uncompressedFeed = await utils.readAndGunzip(pagesPath + '/' + static_version + '/' + new Date(found_fragment).toISOString() + '.jsonld.gz');
            let graph = uncompressedFeed.split(',\n');
            let jsonldGraph = graph.map(JSON.parse);

            for (let k = 0; k < jsonldGraph.length; k++) {
                let connectionURI = jsonldGraph[k]['@id'];
                let connectionTripURI = jsonldGraph[k]['gtfs:trip'];

                if(connectionTripURI === tripURI) {
                    logger.debug('Found canceling connection: ' + connectionURI);
                    //jsonldGraph[k]['@type'] = 'CancelledConnection';
                    foundCancelledConnection = jsonldGraph[k];
                }
            }

            // Save page again to disk
            /*let pageToWrite = pagesPath + '/' + static_version + '/' + new Date(found_fragment).toISOString() + '.jsonld';
            await writeFile(pageToWrite, JSON.stringify(jsonldGraph));

            // Delete any existing .gz file and compress the page
            if(fs.existsSync(pageToWrite + '.gz')) {
                await del([pageToWrite + '.gz']);
            }
            child_process.spawn('gzip', [pathOperations.basename(pageToWrite)], {
                cwd: pathOperations.dirname(pageToWrite),
                stdio: 'ignore'
            });*/

            // Find RT fragment for connection 
            let departureDelay = 0
            if('departureDelay' in foundCancelledConnection) {
                departureDelay = foundCancelledConnection['departureDelay'];
            }
            let connectionDepartureTime = new Date(new Date(foundCancelledConnection['departureTime']).getTime() - departureDelay);
            console.log(new Date(foundCancelledConnection['departureTime']).getTime())
            console.log(departureDelay);
            console.log(new Date(foundCancelledConnection['departureTime']).getTime() - departureDelay);
            let foundRTFragment = null;
            for(let r=0; r < rtFragmentsList.length; r++) {
                currentRTFragment = rtFragmentsList[r];
                currentRTFragmentDate = new Date(pathOperations.basename(currentRTFragment, '.jsonld'));
                if(r < rtFragmentsList.length - 1) {
                    nextRTFragment = rtFragmentsList[r+1];
                    nextRTFragmentDate = new Date(pathOperations.basename(nextRTFragment, '.jsonld'));
                    console.log('NEXT: ' + nextRTFragmentDate.toISOString());
                }
                else {
                    nextRTFragment = null;
                    nextRTFragmentDate = null;
                }
                console.log('CURRENT: ' + currentRTFragmentDate.toISOString());
                console.log('DEPARTURE TIME: ' + connectionDepartureTime.toISOString());

                if(currentRTFragmentDate.getTime() <= connectionDepartureTime.getTime() && (nextRTFragmentDate == null? true: connectionDepartureTime.getTime() < nextRTFragmentDate.getTime())) {
                    console.log('Found RT fragment! ' + currentRTFragment);
                    console.log('Departure time connection:' + connectionDepartureTime.toISOString());
                    console.log('NEXt=' + nextRTFragment);
                    foundRTFragment = currentRTFragment;
                    break;
                }
            }

            // No RT fragment found, we should create it
            if(foundRTFragment == null) {
                let rtTimestamp = new Date(connectionDepartureTime.getTime() - (connectionDepartureTime.getTime() % (dataset.realTimeData.fragmentTimeSpan * 1000)));
                foundRTFragment = rtPath + '/' + rtTimestamp.toISOString() + '.jsonld';
            }

            // Write connection to RT fragment
            fs.writeFileSync(foundRTFragment, JSON.stringify(foundCancelledConnection), { flag: 'a' });

            // Go to the next page
            currentPageTime = new Date(utils.staticFragments[companyName][static_version][index + 1]);
            logger.debug('Going to the next page: ' + currentPageTime);
        }

        logger.debug('Cancelled trip: ' + tripURI + ' from: ' + startingAt + ' until: ' + arrivalPageTime);
        return true;
    }

    downloadDataset(dataset) {
        if (dataset.downloadUrl.startsWith('http')) {
            const durl = url.parse(dataset.downloadUrl);
            if (durl.protocol == 'https:') {
                const options = {
                    hostname: durl.hostname,
                    port: 443,
                    path: durl.path,
                    method: 'GET'
                };

                return this.download_https(dataset, options);
            } else {
                return this.download_http(dataset, durl.href);
            }
        } else {
            return this.copyFileFromDisk(dataset);
        }
    }

    download_https(dataset, options) {
        return new Promise((resolve, reject) => {
            const req = https.request(options, res => {
                try {
                    let file_name = new Date(res.headers['last-modified']).toISOString();
                    let path = this.storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip';

                    if (!fs.existsSync(path)) {
                        let wf = fs.createWriteStream(path, { encoding: 'base64' });

                        res.on('data', d => {
                            wf.write(d);
                        }).on('end', () => {
                            wf.end();
                            wf.on('finish', () => {
                                resolve(file_name);
                            });
                        });
                    } else {
                        resolve(null);
                    }
                } catch (err) {
                    reject(err);
                }
            });

            req.on('error', err => {
                reject(err);
            });
            req.end();
        });
    }

    download_http(dataset, url) {
        return new Promise((resolve, reject) => {
            const req = http.get(url, res => {
                try {
                    let file_name = new Date(res.headers['last-modified']).toISOString();
                    let path = this.storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip';

                    if (!fs.existsSync(path)) {
                        let wf = fs.createWriteStream(path, { encoding: 'base64' });

                        res.on('data', d => {
                            wf.write(d);
                        }).on('end', () => {
                            wf.end();
                            wf.on('finish', () => {
                                resolve(file_name);
                            });
                        });
                    } else {
                        resolve(null);
                    }
                } catch (err) {
                    reject(err);
                }
            });

            req.on('error', err => {
                reject(err);
            });
            req.end();
        });
    }

    copyFileFromDisk(dataset) {
        return new Promise((resolve, reject) => {
            if (fs.existsSync(dataset.downloadUrl) && dataset.downloadUrl.endsWith('.zip')) {
                let stat = fs.statSync(dataset.downloadUrl);
                let name = new Date(util.inspect(stat.mtime)).toISOString();
                let copy = fs.createReadStream(dataset.downloadUrl)
                    .pipe(fs.createWriteStream(this.storage + '/datasets/' + dataset.companyName + '/' + name + '.zip'));
                copy.on('finish', () => {
                    resolve(name);
                });
            } else {
                reject(new Error('Invalid GTFS file'));
            }
        });
    }

    getBaseURIs(dataset) {
        if (dataset.baseURIs && dataset.baseURIs !== '') {
            return dataset.baseURIs;
        } else {
            return {
                'stop': 'http://example.org/stops/{stop_id}',
                'route': 'http://example.org/routes/{routes.route_id}',
                'trip': 'http://example.org/trips/{trips.trip_id}',
                'connection': 'http://example.org/connections/{connection.departureTime(YYYYMMDD)}{connection.departureStop}{trips.trip_id}'
            };
        }
    }

    preSortGTFS(path) {
        return new Promise((resolve, reject) => {
            const child = child_process.spawn('./gtfs2lc-sort.sh', [path], { cwd: './node_modules/gtfs2lc/bin/' });

            child.on('close', (code, signal) => {
                if (signal === 'SIGTERM') {
                    reject(new Error('Process gtfs2lc-sort exit with code: ' + code));
                } else {
                    resolve();
                }
            });
        });
    }

    sortLCByDepartureTime(path, company, file_name) {
        return new Promise((resolve, reject) => {
            // TODO: sorting based on the number of " characters is unreliable and will break when adding or removing fields. 
            // Sorting should happen based on the field name instead.
            child_process.exec('sort -T ' + this.storage + '/tmp/ -t \\" -k 19 ' + this.storage + '/linked_connections/'
                + company + '/' + file_name + '_tmp.jsonld > ' + this.storage + '/linked_connections/' + company + '/' + file_name + '.jsonld',
                (err, stdout, stderr) => {
                    if (err) {
                        logger.error(err);
                        reject();
                    }
                    resolve(this.storage + '/linked_connections/' + company + '/' + file_name + '.jsonld');
                });
        });
    }

    cleanRemoveCache(removeCache, timestamp) {
        // Set time limit to 3 hours before the current moment
        let timeLimit = new Date(timestamp.getTime() - (3 * 3600 * 1000));
        let keys = Object.keys(removeCache);
        for(let i in keys) {
            let k = new Date(keys[i]);
            if(k < timeLimit) {
                delete removeCache[k];
            }
        }

        return removeCache;
    }

    storeRemoveList(removeList, path, memento) {
        Object.entries(removeList).forEach(async ([key, value]) => {
            let file_path = path + '/' + key + '.remove';

            let data = '';
            for (let r in value) {
                let rec = value[r];
                data = data.concat(rec['@id'] + ',' + memento);
                if (rec['track'] && rec['track'] != null) {
                    data = data.concat(',' + rec['track'].join(',') + '\n');
                } else {
                    data = data.concat('\n');
                }
            }

            // Register every connection is not in its original fragment due to delays
            if (fs.existsSync(file_path)) {
                fs.appendFile(file_path, '\n' + data.trim(), 'utf8', err => {
                    if (err) throw err;
                });
            } else {
                fs.appendFile(file_path, data.trim(), 'utf8', err => {
                    if (err) throw err;
                });
            }
        });
    }

    updateRTData(data, path) {
        return new Promise((resolve, reject) => {
            try {
                // Array to store promises for writing fragment files
                let written = [];

                // Update RT fragment files with new data (asynchronously)
                Object.entries(data).forEach(async ([key, value]) => {
                    let updData = value.join('\n');
                    let file_path = path + '/' + key + '.jsonld';

                    if (!fs.existsSync(file_path)) {
                        fs.appendFile(file_path, updData, 'utf8', err => {
                            if (err) throw err;
                        });
                    } else {
                        fs.appendFile(file_path, '\n' + updData, 'utf8', err => {
                            if (err) throw err;
                        });
                    }
                });

                // All RT fragment files updated. RT update completed
                Promise.all(written).then(() => {
                    resolve();
                });
            } catch (err) {
                reject(err);
            }
        });
    }

    cleanUpIncompletes() {
        return Promise.all(this._datasets.map(async dataset => {
            if (fs.existsSync(this.storage + '/datasets/' + dataset.companyName)) {
                let files = await readdir(this.storage + '/datasets/' + dataset.companyName);
                let incomplete = null;

                for (let i in files) {
                    if (files[i].endsWith('.lock')) {
                        incomplete = files[i].substring(0, files[i].indexOf('.lock'));
                        break;
                    }
                }

                if (incomplete !== null) {
                    logger.warn('Incomplete ' + dataset.companyName + ' GTFS feed found (' + incomplete + ')');
                    await del([this.storage + '/datasets/' + dataset.companyName + '/baseUris.json',
                    this.storage + '/datasets/' + dataset.companyName + '/' + incomplete + '_tmp',
                    this.storage + '/datasets/' + dataset.companyName + '/' + incomplete + '.zip',
                    this.storage + '/datasets/' + dataset.companyName + '/' + incomplete + '.lock',
                    this.storage + '/linked_connections/' + dataset.companyName + '/*_tmp*',
                    this.storage + '/linked_pages/' + dataset.companyName + '/' + incomplete], { force: true });
                    logger.info('Incomplete ' + dataset.companyName + ' GTFS feed from ' + incomplete + ' cleaned correctly');
                }
            }
        }));
    }

    get storage() {
        return this._storage;
    }

    get jobs() {
        return this._jobs;
    }

    get indexes() {
        return this._indexes;
    }

    get EventsManager() {
        return this._EventsManager;
    }
}

module.exports = DatasetManager;
