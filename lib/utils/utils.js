const util = require('util');
const fs = require('fs');
const zlib = require('zlib');
const unzip = require('unzipper');
const md5 = require('md5');
const Logger = require('./logger');

const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
var logger = null;

module.exports = new class Utils {

    constructor() {
        this._datasetsConfig = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
        this._serverConfig = JSON.parse(fs.readFileSync('./server_config.json', 'utf8'));
        this._staticFragments = {};
        logger = Logger.getLogger(this._serverConfig.logLevel || 'info');
    }

    async updateStaticFragments() {
        let storage = this._datasetsConfig.storage + '/linked_pages/';
        let datasets = this._datasetsConfig.datasets;

        // Iterate over all companies
        for (let i in datasets) {
            let companyName = datasets[i].companyName;

            // If there is at least on static version available proceed to scan it
            if (fs.existsSync(storage + companyName)) {
                // Object for keeping fragment scan of every static version
                if (!this._staticFragments[companyName]) {
                    this._staticFragments[companyName] = {};
                }

                let versions = await readdir(storage + companyName);
                for (let y in versions) {
                    if (!this._staticFragments[companyName][versions[y]]) {
                        let dir = await readdir(storage + companyName + '/' + versions[y]);
                        let arr = [];
                        // Keep each fragment in milliseconds since epoch in the scan
                        for (let z in dir) {
                            // Check that this version is not being currently converted
                            if (dir[z].indexOf('.gz') >= 0) {
                                let fd = new Date(dir[z].substring(0, dir[z].indexOf('.jsonld')));
                                arr.push(fd.getTime());
                            } else {
                                // Version is being processed now, discard it to avoid incomlete scans 
                                arr = [];
                                break;
                            }
                        }

                        if (arr.length > 0) {
                            this._staticFragments[companyName][versions[y]] = arr;
                        }
                    } else {
                        continue;
                    }
                }
            }
        }
    }

    readAndGunzip(path) {
        return new Promise((resolve, reject) => {
            let buffers = [];
            fs.createReadStream(path)
                .pipe(new zlib.createGunzip())
                .on('error', err => {
                    reject(err + ' - ocurred on file: ' + path);
                })
                .on('data', data => {
                    buffers.push(data);
                })
                .on('end', () => {
                    resolve(buffers.join(''));
                });
        });
    }

    readAndUnzip(path) {
        return new Promise((resolve, reject) => {
            let dirName = path;
            if (dirName.endsWith('.zip')) {
                dirName = dirName.substring(0, dirName.indexOf('.zip')) + '_tmp';
            } else {
                path += '.zip';
            }
            fs.createReadStream(path)
                .pipe(unzip.Extract({ path: dirName }))
                .on('close', () => {
                    resolve(dirName);
                });
        });
    }

    // Sort array of dates ordered from the closest to farest to a given date.
    sortVersions(date, versions) {
        let diffs = [];
        let sorted = [];

        for (let v of versions) {
            let diff = Math.abs(date.getTime() - new Date(v).getTime());
            diffs.push({ 'version': v, 'diff': diff });
        }

        diffs.sort((a, b) => {
            return a.diff - b.diff;
        });

        for (let d of diffs) {
            sorted.push(d.version);
        }

        return sorted;
    }

    // Search for a given fragment across the different static versions
    findResource(agency, target, versions) {
        let version = null;
        let fragment = null;

        for (let i = 0; i < versions.length; i++) {
            let fragments = this.staticFragments[agency][versions[i]];

            // Checking that target date is contained in the list of fragments.
            if (target >= fragments[0] && target <= fragments[fragments.length - 1]) {
                fragment = this.binarySearch(target, fragments);
                version = [versions[i]];
                break;
            } else {
                continue;
            }
        }

        if (version !== null && fragment !== null) {
            return version.concat(fragment);
        } else {
            throw new Error('Fragment not found in current data');
        }
    }

    // Binary search algorithm to find the closest element from the left to a given target, in a sorted numeric array.
    // If the target is not contained in the array it returns null. 
    binarySearch(target, array) {
        let min = 0;
        let max = array.length - 1;
        let index = null;

        // Checking that target is contained in the array.
        if (target >= array[min] && target <= array[max]) {
            // Perform binary search to find the closest, rounded down element to the target in the array .
            while (index === null) {
                // Divide the array in half
                let mid = Math.floor((min + max) / 2);
                // Target date is in the right half
                if (target > array[mid]) {
                    if (target < array[mid + 1]) {
                        index = mid;
                    } else if (target === array[mid + 1]) {
                        index = mid + 1;
                    } else {
                        // Not found yet proceed to divide further this half in 2.
                        min = mid;
                    }
                    // Target date is exactly equal to the middle element
                } else if (target === array[mid]) {
                    index = mid;
                    // Target date is on the left half
                } else {
                    if (target >= array[mid - 1]) {
                        index = mid - 1;
                    } else {
                        max = mid;
                    }
                }
            }
        } else {
            return null;
        }

        return [array[index], index];
    }

    findRTData(agency, lowerLimit, upperLimit) {
        let dataConfig = this.getCompanyDatasetConfig(agency);

        if (dataConfig.realTimeData) {
            let fts = (dataConfig.realTimeData.fragmentTimeSpan || 600) * 1000;
            let rtfs = [];
            let rtfs_remove = [];

            let lowerDate = new Date(lowerLimit - (lowerLimit % fts));
            let upperDate = new Date(upperLimit - (upperLimit % fts));

            // Only one real-time fragment is needed to cover the requested static fragment
            if (lowerDate === upperDate) {
                let path = this.getRTFilePath(lowerDate.toISOString(), agency);
                let path_remove = this.getRTRemoveFilePath(lowerDate.toISOString(), agency);
                if (path !== null) {
                    rtfs.push(path);
                }
                if (path_remove !== null) {
                    rtfs_remove.push(path_remove);
                }
            } else {
                // Get all real-time fragments that cover the requested static fragment
                while (lowerDate.getTime() <= upperDate.getTime()) {
                    let path = this.getRTFilePath(lowerDate.toISOString(), agency);
                    let path_remove = this.getRTRemoveFilePath(lowerDate.toISOString(), agency);
                    if (path !== null) {
                        rtfs.push(path);
                    }
                    if (path_remove !== null) {
                        rtfs_remove.push(path_remove);
                    }
                    lowerDate.setTime(lowerDate.getTime() + fts);
                }
            }

            return [rtfs, rtfs_remove];
        } else {
            return [[], []];
        }
    }

    async aggregateRTData(static_data, rt_data, remove_paths, lowLimit, highLimit, timestamp) {
        // Index map for the static fragment
        let t0 = new Date();
        let static_index = this.getStaticIndex(static_data);
        logger.debug('getStaticIndex() took ' + (new Date().getTime() - t0.getTime()) + ' ms');
        // Index map for the associated real-time fragments
        t0 = new Date();
        let [rt_index, rt_remove] = await this.getRTIndex(rt_data, static_index, lowLimit, highLimit, timestamp);
        logger.debug('getRTIndex() took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        // Array of the Connections that may be removed from the static fragment due to delays
        t0 = new Date();
        let to_remove = await this.getConnectionsToRemove(remove_paths, timestamp);
        logger.debug('getConnectionsToRemove() took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        // Iterate over the RT index which contains all the connections that need to be updated or included
        t0 = new Date();
        for (let [connId, conn] of rt_index) {
            let rtd = null;
            try {
                rtd = JSON.parse(conn);
            } catch (err) {
                continue;
            }
            // If the connection is already present in the static fragment just add delay values and update departure and arrival times
            if (static_index.has(connId)) {
                let std = static_data[static_index.get(connId)];
                std['departureDelay'] = rtd['departureDelay'];
                std['arrivalDelay'] = rtd['arrivalDelay'];
                // Update departure and arrival times with delays
                std['departureTime'] = rtd['departureTime'] || new Date(new Date(std['departureTime']).getTime() + Number(rtd['departureDelay']) * 1000).toISOString();
                std['arrivalTime'] = rtd['arrivalTime'] || new Date(new Date(std['arrivalTime']).getTime() + Number(rtd['arrivalDelay']) * 1000).toISOString();
                static_data[static_index.get(connId)] = std;
            } else {
                // Is not present in the static fragment which means it's a connection that comes from a different fragment 
                // and it is here due to delays.
                delete rtd['mementoVersion'];
                static_data.push(rtd);
                static_index.set(connId, static_data.length - 1);
            }
        }
        logger.debug('Combine static and rt indexes took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        // Now iterate over the arrays of connections that were reported to change real-time fragment due to delays and see 
        // if it necessary to remove them.
        t0 = new Date();

        for (let [connId, timestamp] of to_remove) {
            // Check if they have been reported in real-time updates
            if (rt_index.has(connId)) {
                // Date of the last real-time update registered for this connection within the scope of the requested static fragment
                let rt_memento = new Date(rt_index.get(connId).split('"')[43]);
                // Date of the last remove update registered for this connection
                let remove_memento = new Date(timestamp);
                // If the real-time update is older than the remove update proceed to remove the connection. The reason for this
                // is that an older real-time update means that the connection already has a delay that puts it beyond the range
                // of the requested static fragment.
                if (remove_memento > rt_memento) {
                    let si = static_index.get(connId);
                    static_data.splice(si, 1);
                    static_index = this.getStaticIndex(static_data);
                }
            } else {
                // If the connection is present in the static fragment without an associated real-time update, this means that its
                // real-time updates are beyond the static fragment range so proceed to remove it. 
                if (static_index.has(connId)) {
                    let si = static_index.get(connId);
                    static_data.splice(si, 1);
                    static_index = this.getStaticIndex(static_data);
                }
            }
        }

        // Connections that were identified to be removed during the real-time index creation
        for (let [connId, memento] of rt_remove) {
            if (static_index.has(connId)) {
                let si = static_index.get(connId);
                static_data.splice(si, 1);
                static_index = this.getStaticIndex(static_data);
            }
        }
        logger.debug('Connection removal process took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        // Re-sort the fragment with the updated delay data
        t0 = new Date();
        static_data.sort((a, b) => {
            let a_date = new Date(a['departureTime']).getTime();
            let b_date = new Date(b['departureTime']).getTime();
            return a_date - b_date;
        });
        logger.debug('Re-sorting process took ' + (new Date().getTime() - t0.getTime()) + ' ms');
        return static_data;
    }

    getStaticIndex(fragment) {
        try {
            let map = new Map();
            for (let x in fragment) {
                let conn = fragment[x];
                map.set(conn['@id'], x);
            }
            return map;
        } catch (err) {
            throw err;
        }
    }

    async getRTIndex(arrays, static_index, lowLimit, highLimit, timeCriteria) {
        let map = new Map();
        // Array to keep track of the connections that must be removed due to fragment change withing the range of a single 
        // real-time data fragment. E.g. frag1 = 2018-10-09T02:43:00.000Z, frag2 = 2018-10-09T02:45:00.000Z, 
        // rt-frag = 2018-10-09T02:40:00.000Z (spanning 10 minutes).
        let toRemove = new Map();
        let possibleRemove = new Map();
        let low = new Date(lowLimit);
        let high = new Date(highLimit);

        // Process every real-time fragment asynchronously to speed up the process
        await Promise.all(arrays.map(async array => {
            for (let i in array) {
                let obj = array[i].split('"');
                let memento_date = new Date(obj[43]);

                // Discard invalid dates
                if (memento_date.toString() === 'Invalid Date') {
                    continue;
                }
                // Check that we are dealing with data according to the requested time
                if (memento_date <= timeCriteria) {
                    let connId = obj[3];
                    //Check that this connection belongs to the time range of the requested static fragment
                    let depDate = new Date(obj[19]);
                    if (depDate >= low && depDate < high) {
                        // Check if it has been reported for removal before and if this update is newer
                        if (toRemove.has(connId) && memento_date > toRemove.get(connId)) {
                            toRemove.delete(connId);
                        }
                        if (map.has(connId)) {
                            // Check that this is more updated data
                            let sm = new Date(map.get(connId).split('"')[43]);
                            if (memento_date > sm) {
                                map.set(obj[3], array[i]);
                            }
                        } else {
                            map.set(obj[3], array[i]);
                        }
                    } else {
                        // Check if it was previously added to the real-time index and mark it as possible remove
                        if (map.has(connId)) {
                            if (possibleRemove.has(connId)) {
                                let sm = new Date(possibleRemove.get(connId));
                                if (memento_date > sm) {
                                    possibleRemove.set(connId, memento_date);
                                }
                            } else {
                                possibleRemove.set(connId, memento_date);
                            }
                        }
                        // Check if it originally belongs to the time range of the requested static fragment
                        // for it to be scheduled for removal
                        if (static_index.has(connId)) {
                            toRemove.set(connId, memento_date);
                        }

                    }
                } else {
                    break;
                }
            }
        }));

        // Process the possible removals over the real-time index
        for (let [id, memento] of possibleRemove) {
            let sm = new Date(map.get(id).split('"')[43]);
            if (memento > sm) {
                map.delete(id);
            }
        }

        return [map, toRemove];
    }

    async getConnectionsToRemove(paths, timestamp) {
        let remove_list = new Map();
        // Process every .remove file asynchronously to speed up the process
        await Promise.all(paths.map(async path => {
            // Read .remove file
            let remove_data = null;
            if (path.endsWith('.gz')) {
                remove_data = (await this.readAndGunzip(path)).toString().split('\n');
            } else {
                remove_data = (await readFile(path, 'utf8')).split('\n');
            }

            for (let i in remove_data) {
                let remove = remove_data[i].split(',');
                let memento = new Date(remove[1]);
                if (memento <= timestamp) {
                    remove_list.set(remove[0], memento);
                } else {
                    break;
                }
            }
        }));

        return remove_list;
    }

    async addHydraMetada(params) {
        try {
            let template = await readFile('./statics/pages_skeleton.jsonld', { encoding: 'utf8' });
            let jsonld_skeleton = JSON.parse(template);
            let host = params.host;
            let agency = params.agency;
            let departureTime = params.departureTime;
            let version = params.version;

            jsonld_skeleton['@id'] = host + agency + '/connections?departureTime=' + departureTime.toISOString();

            let next = this._staticFragments[agency][version][Number(params.index) + 1];
            if (next) {
                jsonld_skeleton['hydra:next'] = host + agency + '/connections?departureTime=' + new Date(next).toISOString();
            }

            let prev = this._staticFragments[agency][version][Number(params.index) - 1];
            if (prev && prev !== null) {
                jsonld_skeleton['hydra:previous'] = host + agency + '/connections?departureTime=' + new Date(prev).toISOString();
            }

            jsonld_skeleton['hydra:search']['hydra:template'] = host + agency + '/connections{?departureTime}';
            jsonld_skeleton['@graph'] = params.data;

            params.http_response.set(params.http_headers);
            params.http_response.json(jsonld_skeleton);

        } catch (err) {
            console.error(err);
            throw err;
        }
    }

    /**
    * Checks for Conditional GET requests, by comparing the last modified date and file hash against if-modified-since and if-none-match headers.
    * If a match is made, a 304 response is sent and the function will return true.
    * If the client needs a new version (ie a body should be sent), the function will return false
    * @param req
    * @param res
    * @param filepath The path of the file which would be used to generate the response
    * @param departureTime The time for which this document was requested
    * @returns boolean True if a 304 response is to be served
    */
    handleConditionalGET(req, res, filepath, departureTime, memento) {
        let ifModifiedSinceRawHeader = req.header('if-modified-since');

        let ifModifiedSinceHeader = undefined;
        if (ifModifiedSinceRawHeader !== undefined) {
            ifModifiedSinceHeader = new Date(ifModifiedSinceRawHeader);
        }

        let ifNoneMatchHeader = req.header('if-none-match');

        let stats = fs.statSync(filepath);
        let lastModifiedDate = new Date(util.inspect(stats.mtime));

        let now = new Date();

        // Valid until :31 or :01
        let validUntilDate = new Date();
        validUntilDate.setSeconds(validUntilDate.getSeconds() - (validUntilDate.getSeconds() % 30) + 31);

        let maxage = (validUntilDate - now) / 1000;
        let etag_reference = null;

        // Take into account Memento requests to define the ETag header
        if (memento) {
            etag_reference = filepath + lastModifiedDate + memento;
        } else {
            etag_reference = filepath + lastModifiedDate;
        }

        let etag = 'W/"' + md5(etag_reference) + '"';

        // If both departure time and last modified lie resp. 2 and 1 hours in the past, this becomes immutable
        if (departureTime < (now - 7200000) && lastModifiedDate < (now - 3600000)) {
            // Immutable (for browsers which support it, sometimes limited to https only
            // 1 year expiry date to keep it long enough in cache for the others
            res.set({ 'Cache-control': 'public, max-age=31536000000, immutable' });
            res.set({ 'Expires': new Date(now.getTime() + 31536000000).toUTCString() });
        } else {
            // Let clients hold on to this data for 1 second longer than nginx. This way nginx can update before the clients?
            res.set({ 'Cache-control': 'public, s-maxage=' + maxage + ', max-age=' + (maxage + 1) + ',stale-if-error=' + (maxage + 15) + ', proxy-revalidate' });
            res.set({ 'Expires': validUntilDate.toUTCString() });
        }

        res.set({ 'ETag': etag });
        res.set({ 'Vary': 'Accept-Encoding, Accept-Datetime' });
        res.set({ 'Last-Modified': lastModifiedDate.toUTCString() });
        res.set({ 'Content-Type': 'application/ld+json' });

        // If an if-modified-since header exists, and if the realtime data hasn't been updated since, just return a 304/
        // If an if-none-match header exists, and if the realtime data hasn't been updated since, just return a 304/
        if ((lastModifiedDate !== undefined && ifModifiedSinceHeader !== undefined && ifModifiedSinceHeader >= lastModifiedDate) ||
            (ifNoneMatchHeader !== undefined && ifNoneMatchHeader === etag)) {
            res.status(304).send();
            return true;
        }

        return false;
    }

    getCompanyDatasetConfig(company) {
        let datasets = this._datasetsConfig.datasets
        for (let i in datasets) {
            if (company == datasets[i].companyName) {
                return datasets[i];
            }
        }
    }

    async getLatestGtfsSource(company) {
        let versions = (await readdir(this._datasetsConfig.storage + '/datasets/' + company))
            .reduce((filtered, v) => {
                if (v.endsWith('.zip')) {
                    filtered.push(new Date(v.substring(0, v.indexOf('.zip'))));
                }

                return filtered;
            }, []);

        if (versions.length > 0) {
            versions = versions.filter(v => v.toString() !== 'Invalid Date');
            return this._datasetsConfig.storage + '/datasets/' + company + '/' + this.sortVersions(new Date(), versions)[0].toISOString() + '.zip';
        } else {
            return null;
        }
    }

    getRTFilePath(fragment, company) {
        let path = this._datasetsConfig.storage + '/real_time/' + company + '/';
        if (fs.existsSync(path + fragment + '.jsonld')) {
            return path + fragment + '.jsonld';
        } else if (fs.existsSync(path + fragment + '.jsonld.gz')) {
            return path + fragment + '.jsonld.gz';
        } else {
            return null;
        }
    }

    getRTRemoveFilePath(fragment, company) {
        let path = this._datasetsConfig.storage + '/real_time/' + company + '/';
        if (fs.existsSync(path + fragment + '.remove')) {
            return path + fragment + '.remove';
        } else if (fs.existsSync(path + fragment + '.remove.gz')) {
            return path + fragment + '.remove.gz';
        } else {
            return null;
        }
    }

    get datasetsConfig() {
        return this._datasetsConfig;
    }

    get serverConfig() {
        return this._serverConfig;
    }

    get staticFragments() {
        return this._staticFragments;
    }
}
