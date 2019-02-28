const util = require('util');
const url = require('url');
const fs = require('fs');
const path = require('path');
const chokidar = require('chokidar');
const Logger = require('../utils/logger.js');
const utils = require('../utils/utils.js');
const EventsManager = require('../manager/events_manager.js');
const readDir = util.promisify(fs.readdir);
const readFile = util.promisify(fs.readFile);

class Events {
    constructor() {
        this._listeners = [];
        this._lastPubSubTime = new Date();
        this._eventsStorage = utils.datasetsConfig.storage + '/events';
        this._logger =  Logger.getLogger(utils.serverConfig.logLevel || 'info');
        this._knownAgencies = [];
        this._reconnectionTime = utils.datasetsConfig.reconnectionTime;
        /* 
         * Attach a file watcher to the events directory to generate the events for SSE clients.
         * We ignore any initial files and hidden files.
         */
        this._watcher = chokidar.watch(this.eventsStorage, { ignored: /(^|[\/\\])\../, ignoreInitial: true }).on('add', async (file, info) => {
            await this._handleWatcher(this, file, info);
        });
    }

    /**
     * Handles the file watcher events by triggering a SSE push.
     * @param self `this` access in callback.
     * @param file The file path of the file event.
     * @param info Information about the file event.
     */
    async _handleWatcher(self, file, info) {
        let t0 = new Date();
        let t1;
        let eventsFileDate = new Date(path.basename(file, '.jsonld'));
        if(self.lastPubSubTime.getTime() < eventsFileDate.getTime()) {
            self.lastPubSubTime = eventsFileDate;
            let agency = path.basename(path.dirname(file));
            // Only push events if clients are listening for the agency events
            if(agency in self.listeners && self.listeners[agency].length > 0) {
                self.logger.debug(`Found a new events file: ${eventsFileDate.toISOString()}.jsonld for agency: ${agency}, publishing to SSE clients...`);
                await self._handlePubSub(agency, file);
                t1 = new Date();
                this.logger.debug(`Pushing events to clients took: ${t1.getTime() - t0.getTime()} ms`);
            }
        }
    }

    /**
     * Dispatches the events requests to the right handlers and checks if the request is valid.
     * @param req The request object
     * @param res The response object
     * @author Dylan Van Assche
     */
    async getEvents(req, res) {
        let t0 = new Date();
        let t1;
        let lastSyncTime = new Date(decodeURIComponent(req.query.lastSyncTime));

        // Redirect to NOW time in case provided date is invalid, missing events are ignored.
        if (lastSyncTime.toString() === 'Invalid Date') {
            this.logger.warn('Invalid data received, unable to retrieve missing events');
            lastSyncTime = new Date();
        }

        let agency = req.params.agency;
        // Check if we have this agency already in our cache
        if(this.knownAgencies.indexOf(agency) > -1) {
            // Maybe this agency was recently added, check it before rejecting
            let dir = path.join(this.eventsStorage, agency);
            if(!fs.existsSync(dir)) {
                this.logger.error(`Unable to retrieve events for agency: ${agency}, agency doesn't exist`);
                res.status(404);
                res.json({
                    'error': 404,
                    'message': `Agency ${agency} not found`
                });
                return;
            }
            // New agency, push it to the cache
            else {
                this.knownAgencies.push(agency);
            }
        }

        // Avoid big sync requests (longer than a day), reject them using HTTP 400
        if(new Date().getTime() - lastSyncTime.getTime() > 24*60*60*1000) {
            res.status(400);
            res.json({
                'error': 400,
                'message': "Bad request: you're too far out of sync (longer than a day). Please redownload your pages."
            });
            return;
        }

        // Allow CORS
        res.setHeader('Access-Control-Allow-Origin', '*');

        if(req.headers.accept.indexOf('text/event-stream') > -1) {
            this.logger.debug('SSE client connected');
            await this._addListenerPubSub(req, res, lastSyncTime, agency);
        }
        else {
            this.logger.debug('HTTP polling client connected');
            await this._handlePolling(req, res, lastSyncTime, agency);
        }

        t1 = new Date();
        this.logger.info(`Handling events client (totally) took: ${t1.getTime() - t0.getTime()} ms`);
    }

    /**
     * Adds a client to the listeners queue of an agency.
     * The client gets an individual update between now and its lastSyncTime.
     * @param req The request object
     * @param res The response object
     * @param lastSyncTime The last time the client received an update (Date object).
     * @param agency The agency for which the client wants to receive an update.
     * @private
     * @author Dylan Van Assche
     */
    async _addListenerPubSub(req, res, lastSyncTime, agency) {
        let t0 = new Date();
        let t1;

        // Set the required HTTP headers for SSE
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        res.setHeader('Expires', 0);
        res.setHeader('Connection', 'keep-alive');

        // Generate listeners queue for new agencies, the client can be the first one who requests an agency
        if(!(agency in this.listeners)) {
            this.logger.debug(`Pubsub request for a new agency: ${agency}, creating listeners queue...`)
            this.listeners[agency] = [];
        }
        
        // Send missing events since lastSyncTime (only for this listener), before adding to the listeners queue
        let currentPage = null;
        let currentPageTime = null;
        let syncTime = lastSyncTime;
        let now = new Date()
        while(syncTime.getTime() < now.getTime()) {
            // Get current page and its events time
            currentPage = await this._getEventsPage(req, res, syncTime, agency); 
            currentPageTime = url.parse(currentPage['@id'], true).query['lastSyncTime'];

            // Push it to the client
            this._pushToClient(res, currentPageTime, currentPage);

            // When we're at the end of our event pages, no hydra:next is available, if not, find the next page
            if('hydra:next' in currentPage) {
                syncTime = new Date(url.parse(currentPage['hydra:next'], true).query['lastSyncTime']);
                this.logger.debug(`Finding the next events page: ${syncTime.toISOString()}`);
            }
            else {
                this.logger.debug('No hydra:next available');
                break;
            }
        }

        // Add the listener to the pool, attach the 'close' event to remove the listener when the connection is closed.
        this.listeners[agency].push(res);
        res.on('close', () => {
            this.listeners[agency].splice(this.listeners[agency].indexOf(res), 1);
        });
        
        t1 = new Date();
        this.logger.info('Adding and syncing pubsub client took: ' + (t1.getTime() - t0.getTime()) + ' ms' );
    }

    /**
     * Handles a pubsub client by pushing the new data to the client when the events are generated by the EventsManager.
     * @param file The path of the new events file.
     * @param agency The agency for which the client wants to receive an update.
     * @private
     * @author Dylan Van Assche
     */
    async _handlePubSub(agency, file) {
        // Read events file
        let page = await readFile(file);
        page = JSON.parse(page);
        let currentPageTimestamp = new Date(path.basename(file, '.jsonld'));
        
        // Determine protocol (i.e. HTTP or HTTPS)
        let protocol = '';
        if(typeof utils.serverConfig.protocol == 'undefined' || utils.serverConfig.protocol == '') {
            protocol = 'http';
        } else {
            protocol = utils.serverConfig.protocol;
        }

        let host = protocol + '://' + utils.serverConfig.hostname + '/';
        let eventsURI = host + agency + '/events?lastSyncTime=';
        let templateURI = host + agency + '/events{?lastSyncTime}';
        let pageURI = host + agency + '/connections?departureTime=';

        // Update graph
        page['@id'] = eventsURI + currentPageTimestamp.toISOString();
        page['hydra:search']['hydra:template'] = templateURI;
        
        // Add the data to the graph
        let graph = page['@graph'];
        for(let g=0; g < graph.length; g++) {
            graph[g]['hydra:view'] = pageURI + graph[g]['sosa:hasResult']['Connection']['departureTime'];
        }

        // Publish SSE events for all listeners
        this.listeners[agency].forEach((client) => {
            this._pushToClient(client, currentPageTimestamp, page);
        });
    }

    /**
     * Handles a polling client by serving the right page depending on the `lastSyncTime`.
     * @param res The response object
     * @param req The request object
     * @param lastSyncTime The last time the client received an update (Date object).
     * @param agency The agency for which the client wants to receive an update.
     * @private
     * @author Dylan Van Assche
     */
    async _handlePolling(req, res, lastSyncTime, agency) {
        let t0 = new Date();
        let t1;

        // Set the required HTTP headers for polling
        res.setHeader('Cache-Control', 'max-age=' + utils.datasetsConfig.realTimeUpdateInterval);

        // Send missing events since lastSyncTime
        let events = await this._getEventsPage(req, res, new Date(lastSyncTime), agency);
        if(events !== null) {
            res.json(events);
        }
        else {
            res.status(404);
            res.json({
                'error': 404,
                'message': `Page for timestamp: ${lastSyncTime.toISOString()} cannot be found`
            });
        }
        t1 = new Date();
        this.logger.info('Polling client handling took: ' + (t1.getTime() - t0.getTime()) + ' ms' );
    }

    /**
     * Gets all the generated events for the `when` timestamp.
     * @param when the timestamp of the events we need.
     * @param agency the company for which we're publishing events.
     * @return page If a page answers the search query, the page will be returned. In any other case, `null` is returned.
     * @private
     * @author Dylan Van Assche
     */
    async _getEventsPage(req, res, when, agency) {
        // Check all published events and compile a list of all events in the given time range.
        let page = {};
        let dir = path.join(this.eventsStorage, agency);
        let entries = await readDir(dir);
        
        // Binary search of target
        let target = when.getTime();
        let array = [];
        for(let e=0; e < entries.length; e++) {
            let t = new Date(path.basename(entries[e], '.jsonld'));
            array.push(t.getTime());
        }
        array.sort(); // Sorted array is required for binary search
        
        // Target time is later than our latest event file, redirect the client to this file
        if(array[array.length-1] < target) {
            let lastPageTimestamp = new Date(array[array.length-1]);
            res.location('/' + agency + '/events?lastSyncTime=' + lastPageTimestamp.toISOString());
            res.status(302).send();
            return;
        }

        // We should start looking for the page using a binary search
        let result = utils.binarySearch(target, array);
        let eventsDatePrevious = null;
        let eventsDateCurrent = null;
        let eventsDateNext = null;
        if(result !== null) {
            let pageTimestamp = result[0];
            let index = result[1];
       
            if(index > 1) {
                eventsDatePrevious = new Date(array[index - 1]);
            }
            
            if(index < array.length-1) {
                eventsDateNext = new Date(array[index + 1]);
            }

            eventsDateCurrent = new Date(pageTimestamp); 
            let page = eventsDateCurrent.toISOString() +  '.jsonld';
            page = await readFile(path.join(dir, page));
            page = JSON.parse(page);
            page = this._addHydraMetaData(req, res, page, agency, eventsDatePrevious, eventsDateCurrent, eventsDateNext);
            return page;
        }

        // Linear search
        /*for(let e=0; e < entries.length; e++) {
            let eventsDatePrevious = null;
            let eventsDateCurrent = new Date(path.basename(entries[e], '.jsonld'));
            let eventsDateNext = null;
            
            if(e > 1) {
                eventsDatePrevious = new Date(path.basename(entries[e-1], '.jsonld'));
            }
            
            if(e < entries.length-1) {
                eventsDateNext = new Date(path.basename(entries[e+1], '.jsonld'));
            }
            
            /*
             *  The requested sync time must be on the current page or just before the next one. 
             *  In case we're at the end of our pages, we don't have a next page (== null)
             */
          /*  if(eventsDateCurrent.getTime() <= when.getTime() && (eventsDateNext == null? true: when.getTime() < eventsDateNext.getTime())) {
                page = await readFile(path.join(dir, entries[e]));
                page = JSON.parse(page);
                page = this._addHydraMetaData(req, res, page, agency, eventsDatePrevious, eventsDateCurrent, eventsDateNext);
                return page;
            }
        }*/

        this.logger.error(`No page found for timestamp: ${when.toISOString()}`);
        return null;
    }

    /**
     * Add hydra meta data to the page.
     * @param req The request object.
     * @param res The response object.
     * @param page The page without meta data.
     * @param agency The agency related to the page.
     * @param previousPageTimestamp The previous page timestamp.
     * @param currentPageTimestamp The current page timestamp.
     * @param nextPageTimestamp The next page timestamp.
     * @return page The page with meta data.
     * @private
     * @author Dylan Van Assche
     */
    _addHydraMetaData(req, res, page, agency, previousPageTimestamp, currentPageTimestamp, nextPageTimestamp) {
        // Determine protocol (i.e. HTTP or HTTPS)
        let xForwardedProto = req.headers['x-forwarded-proto'];
        let protocol = '';

        if (typeof xForwardedProto == 'undefined' || xForwardedProto == '') {
            if (typeof utils.serverConfig.protocol == 'undefined' || utils.serverConfig.protocol == '') {
                protocol = 'http';
            } else {
                protocol = utils.serverConfig.protocol;
            }
        } else {
            protocol = xForwardedProto;
        }

        let host = protocol + '://' + utils.serverConfig.hostname + '/';
        let eventsURI = host + agency + '/events?lastSyncTime=';
        let templateURI = host + agency + '/events{?lastSyncTime}';
        let pageURI = host + agency + '/connections?departureTime=';

        // Update graph
        page['@id'] = eventsURI + currentPageTimestamp.toISOString();
        page['hydra:search']['hydra:template'] = templateURI;

        // If no previous page is available, remove it
        if(previousPageTimestamp == null) {
            this.logger.debug('No previous page available');
            delete page['@context']['hydra:previous'];
            delete page['hydra:previous'];
        }
        else {
            page['hydra:previous'] = eventsURI + previousPageTimestamp.toISOString();
        }

        // If no next page is available, remove it
        if(nextPageTimestamp == null) {
            this.logger.debug('No next page available');
            delete page['@context']['hydra:next'];
            delete page['hydra:next'];
        }
        else {
            page['hydra:next'] = eventsURI + nextPageTimestamp.toISOString();
        }

        // Add the data to the graph
        let graph = page['@graph'];
        for(let g=0; g < graph.length; g++) {
            graph[g]['hydra:view'] = pageURI + graph[g]['sosa:hasResult']['Connection']['departureTime'];
        }

        return page;
    }

    /**
     * Push the given data to the client according to the SSE protocol.
     * @param client The client (response object) who wants to receive the data.
     * @param id The id of the message, usefull for resyncing in case something goes wrong.
     * @param data The data of the message.
     * @param eventType The type of event, default `message`. This can be used to ping the clients too!
     * @private
     * @author Dylan Van Assche
     */
    _pushToClient(client, id, data, eventType='message') {
        // Remove hydra navigation metadata since we don't use it for pubsub
        let dataCloned = JSON.parse(JSON.stringify(data)); // Do not modify the original data
        delete dataCloned['@context']['hydra:next'];
        delete dataCloned['@context']['hydra:previous'];
        delete dataCloned['hydra:next'];
        delete dataCloned['hydra:previous'];

        // Push to client
        client.write(`id: ${id}\n`);
        client.write(`event: ${eventType}\n`);
        client.write(`retry: ${this.reconnectionTime}\n`);
        client.write(`data: ${JSON.stringify(dataCloned)}\n`);
        client.write('\n'); // Indicate end
    }

    get listeners() {
        return this._listeners;
    }

    set listeners(l) {
        this._listeners = l;
    }

    get lastPubSubTime() {
        return this._lastPubSubTime;
    }

    set lastPubSubTime(t) {
        this._lastPubSubTime = t;
    }

    get logger() {
        return this._logger;
    }

    get eventsStorage() {
        return this._eventsStorage;
    }

    get watcher() {
        return this._watcher;
    }

    get reconnectionTime() {
        return this._reconnectionTime;
    }

    get knownAgencies() {
        return this._knownAgencies;
    }
}

module.exports = Events;
