const { createReadStream, createWriteStream } = require('fs');
const { createGzip } = require('zlib');

const logger = require('../logging');

module.exports = function(config) {


    const configured = Boolean(config.directory);

    return {
        put(key, filename) {
            return new Promise((resolve, reject) => {
                if (!configured) {
                    logger.warn('[File] No directory configured for storage');

                    return resolve(); // not an error.
                }
                const handleStream = createReadStream(filename);

                handleStream
                    .pipe(createGzip())
                    .pipe(createWriteStream(`${config.directory}/${key}.gz`))
                    .on('error', err => reject(err))
                    .on('finish', () => {
                        resolve();
                    });
            });
        }
    };
};
