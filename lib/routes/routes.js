const fs = require('fs');
const csv = require('fast-csv');
const util = require('util');
const uri_templates = require('uri-templates');
const del = require('del');
const utils = require('../utils/utils');
const { GTFS_ROUTE_TYPES } = require('../utils/constants');

const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);

class Routes {

    constructor(source) {
        this._storage = utils.datasetsConfig.storage;
        this._serverConfig = utils.serverConfig;
        this._source = source || null;
    }

    async getRoutes(req, res) {
        try {
            const agency = req.params.agency;
            if (utils.getCompanyDatasetConfig(agency)) {
                if (fs.existsSync(`${this.storage}/routes/${agency}/routes.json`)) {
                    res.set({
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': '*',
                        'Content-Type': 'application/ld+json'
                    });
                    res.send(await readFile(`${this.storage}/routes/${agency}/routes.json`, 'utf8'));
                } else {
                    let routes = await this.createRouteList(agency);
                    if (routes != null) {
                        writeFile(`${this.storage}/routes/${agency}/routes.json`, JSON.stringify(routes), 'utf8');
                        res.set({
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Headers': '*',
                            'Content-Type': 'application/ld+json'
                        });
                        res.send(routes);
                        return;
                    } else {
                        res.set({ 'Cache-Control': 'no-cache' });
                        res.status(404).send("No stops available for " + agency);
                    }
                }
            } else {

            }
        } catch (err) {
            console.error(err);
            res.set({ 'Cache-Control': 'no-cache' });
            res.status(500).send(`Internal error when getting route list for ${agency}`);
        }
    }

    createRouteList(company) {
        return new Promise(async (resolve, reject) => {
            let dataset = this.getDataset(company);
            let feed = await utils.getLatestGtfsSource(`${this.storage}/datasets/${company}`);

            if (feed) {
                let skeleton = {
                    "@context": {
                        "dct": "http://purl.org/dc/terms/",
                        "xsd": "http://www.w3.org/2001/XMLSchema#",
                        "gtfs": "http://vocab.gtfs.org/terms#",
                        "Route": "gtfs:Route",
                        "shortName": {
                            "@id": "gtfs:shortName",
                            "@type": "xsd:string"
                        },
                        "longName": {
                            "@id": "gtfs:longName",
                            "@type": "xsd:string"
                        },
                        "routeType": {
                            "@id": "gtfs:routeType",
                            "@type": "@id"
                        },
                        "routeColor": {
                            "@id": "gtfs:color",
                            "@type": "xsd:string"
                        },
                        "textColor": {
                            "@id": "gtfs:textColor",
                            "@type": "xsd:string"
                        },
                        "description": {
                            "@id": "dct:description",
                            "@type": "xsd:string"
                        }
                    },
                    "@graph": []
                };

                let uncompressed = this.source || await utils.readAndUnzip(feed);
                let routes_uri_template = uri_templates(dataset['baseURIs']['route']);
                let res = dataset['baseURIs']['resolve'];
                let tripsIndex = await this.getTripsIndex(uncompressed);

                fs.createReadStream(`${uncompressed}/routes.txt`, { encoding: 'utf8', objectMode: true })
                    .pipe(csv.parse({ objectMode: true, headers: true }))
                    .on('data', route => {
                        let trip = tripsIndex.get(route['route_id']);
                        let obj = {
                            "@id": utils.resolveURI(routes_uri_template, { route: route, trip: trip }, res),
                            "@type": "Route",
                            "shortName": route['route_short_name'] ? route['route_short_name'].trim() : null,
                            "longName": route['route_long_name'] ? route['route_long_name'].replace('--', '–').trim() : null,
                            "routeColor": route['route_color'] ? route['route_color'].trim() : null,
                            "textColor": route['route_text_color'] ? route['route_text_color'].trim() : null,
                            "description": route['route_desc'] ? route['route_desc'].trim() : null,
                            "routeType": GTFS_ROUTE_TYPES[route['route_type']] || null
                        };

                        obj = this.cleanEmpties(obj);
                        skeleton['@graph'].push(obj);
                    }).on('error', err => {
                        reject(err);
                    }).on('end', async () => {
                        if (!this.source) {
                            await del([uncompressed], { force: true });
                        }
                        resolve(skeleton);
                    });

            } else {
                resolve(null);
            }
        });
    }

    getDataset(name) {
        let datasets = utils.datasetsConfig.datasets;
        for (let i in datasets) {
            if (datasets[i].companyName === name) {
                return datasets[i];
            }
        }
    }

    getTripsIndex(path) {
        return new Promise((resolve, reject) => {
            let map = new Map();
            fs.createReadStream(`${path}/trips.txt`, { encoding: 'utf8', objectMode: true })
                .pipe(csv.parse({ objectMode: true, headers: true }))
                .on('data', trip => {
                    map.set(trip['route_id'], trip);
                })
                .on('error', err => reject(err))
                .on('end', () => resolve(map));
        });
    }

    cleanEmpties(obj) {
        let keys = Object.keys(obj);
        for (let i in keys) {
            if (!obj[keys[i]] || obj[keys[i]] === '') {
                delete obj[keys[i]];
            }
        }

        return obj;
    }

    get storage() {
        return this._storage;
    }

    get serverConfig() {
        return this._serverConfig;
    }

    get source() {
        return this._source;
    }
}

module.exports = Routes;