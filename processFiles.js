// processFiles.js

const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const createKey = require('./createKey');

async function readCSVFile(file, channel, columns, keys, data, combinedData, globalUniqueKeys) {
    let registrosCargados = 0; // Variable para contar registros
    let isFirstLine = true; // Bandera para identificar la primera línea (encabezados)

    return new Promise((resolve, reject) => {
        fs.createReadStream(file)
            .pipe(csv({ headers: columns }))
            .on('data', async (row) => {
                if (!isFirstLine) { // Ignorar la primera línea (encabezados)
                    registrosCargados++; // Incrementar contador de registros

                    try {
                        const combinedKey = await createKey(row, keys); // Esperar a que se resuelva la promesa
                        if (!globalUniqueKeys.has(combinedKey)) {
                            globalUniqueKeys.add(combinedKey); // Agregar al conjunto global de claves únicas
                        }
                        if (!combinedData[combinedKey]) {
                            combinedData[combinedKey] = { ...row, sources: { P7: false, P9: false, PRD: false } };
                        }
                        combinedData[combinedKey].sources[channel] = true;
                    } catch (error) {
                        reject(error);
                    }
                } else {
                    isFirstLine = false; // Desactivar la bandera después de la primera línea
                }
            })
            .on('end', () => {
                console.log(`Archivo ${file} cargado.`);
                console.log(`Total de registros: ${registrosCargados}`);
                console.log(`Total de registros únicos cargados: ${globalUniqueKeys.size}`);
                resolve({ combinedData, globalUniqueKeys });
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}


async function processFiles(library, table, country, columns, keys) {
    const channels = ['P7', 'P9', 'PRD'];
    let data = { P7: [], P9: [], PRD: [] };
    let combinedData = {};
    let globalUniqueKeys = new Set(); // Conjunto global para rastrear claves únicas

    for (let channel of channels) {
        let filePath;
        if (channel === 'PRD') {
            filePath = path.join(__dirname, 'data', `${library}_${channel}_${country}_${table}.csv`);
            filePath = filePath.replace('QA_', 'PR_');
        } else {
            filePath = path.join(__dirname, 'data', `${library}_${channel}_${table}.csv`);
        }

        if (!fs.existsSync(filePath)) {
            console.error(`El archivo ${filePath} no existe.`);
            process.exit(1);
        }

        const { combinedData: updatedCombinedData, globalUniqueKeys: updatedGlobalUniqueKeys } = await readCSVFile(filePath, channel, columns, keys, data, combinedData, globalUniqueKeys);
        combinedData = updatedCombinedData;
        globalUniqueKeys = updatedGlobalUniqueKeys;
    }

    return { combinedData, globalUniqueKeys };
}

module.exports = processFiles;
