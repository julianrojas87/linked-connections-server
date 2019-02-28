const utils = require('../utils/utils.js');
const Logger = require('../utils/logger');
const fs = require('fs');
const util = require('util');
const readFile = util.promisify(fs.readFile);

class EventsManager {
    constructor() {
        this._previousEvents = {};
        this._pendingEvents = {};
        this._eventsStorage = utils.datasetsConfig.storage + '/events';
        this._logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');
    }

    /**
     * Add an event for publication. The manager will automatically publish it later.
     * @param pageURI where the connection can be found.
     * @param connection which describes the information of the event (connection).
     * @param agency the agency of the connection.
     */
    addEvent(connection, agency) {
        // Check if we're pushing valid events to the queue
        if(typeof connection === 'undefined' || connection === null) {
            this.logger.error('Invalid connection, unable to push event to pending events!');
            return;
        }
        if(typeof agency === 'undefined' || agency === null) {
            this.logger.error('Invalid agency name, unable to push event to pending events!');
            return;
        }

        // Create agency if it doesn't exist yet
        if(!(agency in this.pendingEvents)) {
            this.logger.debug(`Events received for a new agency: ${agency}, creating storage...`)
            this.pendingEvents[agency] = [];
            this.previousEvents[agency] = [];
            // NodeJS 10.X.X or later
            if (!fs.existsSync(this.eventsStorage + '/' + agency)) {
                fs.mkdirSync(this.eventsStorage + '/' + agency, { recursive: true });
            }
        }

        // Event is already published, check if the event has been updated
        let isNew = false;
        let existingEvent = this.previousEvents[agency].find(event => event['connection']['@id'] === connection['@id']);

        // Already generated this event, check if it's updated...
        if(typeof existingEvent !== 'undefined') {
            let type = existingEvent['connection']['@type'];
            let departureDelay = existingEvent['connection']['departureDelay'];
            let arrivalDelay = existingEvent['connection']['arrivalDelay'];
            if(type !== connection['@type'] || departureDelay !== connection['departureDelay'] || arrivalDelay !== connection['arrivalDelay']) {
                isNew = true;
                this.logger.debug('Existing event has been updated for connection: ' + connection['@id']);
                if(type !== connection['@type']) {
                    this.logger.debug('Reason: ' + type + ' -> ' + connection['@type']);
                }

                if(departureDelay !== connection['departureDelay']) {
                    this.logger.debug('Reason: departure delay ' + departureDelay + ' min -> ' + connection['departureDelay'] + ' min');
                }

                if(arrivalDelay !== connection['arrivalDelay']){
                    this.logger.debug('Reason: arrival delay ' + arrivalDelay + ' min -> ' + connection['arrivalDelay'] + ' min');
                }

                // Existing event updated, removing the old one
                this.previousEvents[agency].splice(this.previousEvents[agency].indexOf(existingEvent), 1);
            }
        }
        // New event
        else {
            isNew = true;
        }

        // Add event to the queue if changed, update the previousEvents with the new event
        if(isNew) {
            this.pendingEvents[agency].push({
                id: new Date(),
                connection: connection,
            });
            this.previousEvents[agency].push({
                referenced: true,
                connection: connection
            });
        }
        // Event hasn't been referenced in the current GTFS-RT feed
        else {
            let index = this.previousEvents[agency].indexOf(existingEvent);
            this.previousEvents[agency][index]['referenced'] = false; // Needed for clean up
        }
    }

    /**
     * Callback method which automatically publishes the events that are pending after the `setInterval` timer has run out.
     * @param timestamp Timestamp for the GTFS-RT version (Date object)
     */
    async processEvents(timestamp) {
        // Skeleton of the event publication
        let template = await readFile('./statics/events_skeleton.jsonld', { encoding: 'utf8' });
        let skeleton = JSON.parse(template);
        timestamp = new Date(timestamp.setMilliseconds(0)); // Remove microseconds from timestamp 

        // Read each pending event, add it to the knowledge graph
        for(let pE=0; pE < Object.keys(this.pendingEvents).length; pE++) {
            let pendingEventsForAgencyCounter = 0;
            let agency = Object.keys(this.pendingEvents)[pE];

            // NodeJS 10.X.X or later
            if (!fs.existsSync(this.eventsStorage + '/' + agency)) {
                fs.mkdirSync(this.eventsStorage + '/' + agency, { recursive: true });
            }

            let pendingEventsForAgency = this.pendingEvents[agency];
            if(pendingEventsForAgency.length === 0) {
                this.logger.debug(`No events pending for agency: ${agency}`)
                continue;
            }

            // We have at least one event, use the first event departure time (without the departure delay) as name of the file.
            while (pendingEventsForAgency.length > 0) {
                // Keep order of publication using .shift()
                let eventData = pendingEventsForAgency.shift();
                // Add LOD vocabulary for each event
                let event = {
                    '@id': eventData['connection']['@id'] + '#' + eventData['id'].toISOString(),
                    '@type': 'Event',
                    'hydra:view': '',
                    'sosa:resultTime': eventData['id'],
                    'sosa:hasResult': {
                        '@type': 'sosa:hasResult',
                        'Connection': eventData['connection']
                    }
                };
                skeleton['@graph'].push(event);
                pendingEventsForAgencyCounter++;
            }

            // Process the publication of the events
            let publicationFileStream = fs.createWriteStream(this.eventsStorage + '/'
                + agency + '/' + timestamp.toISOString() + '.jsonld', { flags:'w' });
            publicationFileStream.write(JSON.stringify(skeleton));
            publicationFileStream.end();
            this.logger.debug(`Publication of ${pendingEventsForAgencyCounter} pending events (${timestamp.toISOString()}.jsonld) successfully for agency: ${agency}`);

            // Clean up non-referenced events in previous events list
            for(let e=0; e < this.previousEvents[agency]; e++) {
                if(!this.previousEvents[agency][e]['connection']['referenced']) {
                    this.previousEvents[agency].splice(this.previousEvents[agency].indexOf(e), 1);
                    this.logger.debug('Removed previous events (non-referenced):' + this.previousEvents[agency][e]['connection']['@id']);
                }
            }
        }
    }

    get pendingEvents() {
        return this._pendingEvents;
    }

    set pendingEvents(pE) {
        this._pendingEvents = pE;
    }

    get eventsStorage() {
        return this._eventsStorage;
    }

    get logger() {
        return this._logger;
    }

    get agency() {
        return this._agency;
    }

    get previousEvents() {
        return this._previousEvents;
    }
}

module.exports = EventsManager;
