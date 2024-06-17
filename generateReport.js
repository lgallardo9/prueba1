// generateReport.js

const fs = require('fs');
const ejs = require('ejs');
const path = require('path');
const createKey = require('./createKey');


async function generateReport(library, table, country, Descripcion, Modulo, columns, keys, combinedData, globalUniqueKeys, onlyChanges, onlyPRD) {
    try {
        // Asegúrate de que combinedData esté definido y no sea null
        if (!combinedData) {
            throw new Error('El objeto combinedData no está definido o es null.');
        }

        const rows = Object.values(combinedData).map((row) => {
            const discrepancies = {};
            let hasDiscrepancy = false;

            for (let field of columns.slice(keys.length)) { // Saltar las columnas clave
                const P7Value = row.sources.P7 ? row[field] : 'No existe';
                const P9Value = row.sources.P9 ? row[field] : 'No existe';
                const PRDValue = row.sources.PRD ? row[field] : 'No existe';

                const P7P9Diff = P7Value !== P9Value;
                const P7PRDDiff = P7Value !== PRDValue;
                const P9PRDDiff = P9Value !== PRDValue;

                discrepancies[field] = {
                    P7: P7Value,
                    P9: P9Value,
                    PRD: PRDValue,
                    differenceP7P9: P7P9Diff ? 'Diferente' : 'Igual',
                    differenceP7PRD: P7PRDDiff ? 'Diferente' : 'Igual',
                    differenceP9PRD: P9PRDDiff ? 'Diferente' : 'Igual'
                };

                if (!row.sources.P7 || !row.sources.P9 || !row.sources.PRD || P7P9Diff || P7PRDDiff || P9PRDDiff) {
                    hasDiscrepancy = true;
                }
            }

            return {
                ...row,
                discrepancies,
                hasDiscrepancy
            };
        });

        let filteredRows = rows;
        if (onlyChanges) {
            filteredRows = filteredRows.filter(row => row.hasDiscrepancy);
        }

        if (onlyPRD) {
            filteredRows = filteredRows.filter(row => row.sources.PRD);
        }

        // Métrica 1: Cantidad de registros unificados entre ambientes
        const registrosUnificados = globalUniqueKeys.size; // Usar size para obtener la cantidad de elementos en un Set

        // Métrica 2: Cantidad de registros con diferencias entre P7 y P9 y su %
        const registrosDiferenciaP7P9 = filteredRows.filter(row => row.discrepancies[columns[keys.length]].differenceP7P9 === 'Diferente').length;
        const porcentajeDiferenciaP7P9 = registrosDiferenciaP7P9 / registrosUnificados * 100;

        // Métrica 3: Cantidad de registros con diferencias entre P7 y PRD y su %
        const registrosDiferenciaP7PRD = filteredRows.filter(row => row.discrepancies[columns[keys.length]].differenceP7PRD === 'Diferente').length;
        const porcentajeDiferenciaP7PRD = registrosDiferenciaP7PRD / registrosUnificados * 100;

        // Métrica 4: Cantidad de registros con diferencias entre P9 y PRD y su %
        const registrosEnPRD = filteredRows.filter(row => row.sources.PRD).length;
        const registrosDiferenciaPRDP9 = filteredRows.filter(row => row.discrepancies[columns[keys.length]].differenceP9PRD === 'Diferente').length;
        const porcentajeDiferenciaPRDP9 = registrosDiferenciaPRDP9 / registrosUnificados * 100;

        // Métrica adicional: Cantidad de registros con cualquier diferencia y su %
        const registrosConCualquierDiferencia = filteredRows.filter(row => row.hasDiscrepancy).length;
        const porcentajeRegistrosConCualquierDiferencia = registrosConCualquierDiferencia / registrosUnificados * 100;

        // Ordenar las filas si es necesario
        filteredRows.sort((a, b) => {
            const keyA = createKey(a, keys).toString(); // createKey debe devolver directamente un string, no una promesa
            const keyB = createKey(b, keys).toString(); // createKey debe devolver directamente un string, no una promesa
            return keyA.localeCompare(keyB);
        });

        const outputDir = path.join(__dirname, 'out');
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir);
        }

        const outputPath = path.join(outputDir, `${library}_${table}_report.html`);

        const now = new Date();
        const formattedDate = now.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
        const formattedTime = now.toTimeString().slice(0, 5);
        const dateTimeString = `${formattedDate} ${formattedTime}`;

        const filesInvolved = ['P7', 'P9', 'PRD'].map(channel => {
            if (channel === 'PRD') {
                return `${library}_${channel}_${country}_${table}.csv`;
            } else {
                return `${library}_${channel}_${table}.csv`;
            }
        }).join(', ');

        const filtersApplied = [
            onlyChanges ? 'Only Changes' : null,
            onlyPRD ? 'Only PRD Present' : null
        ].filter(Boolean).join(', ');

        const title = `Reporte de Tabla ${table}`;

        try {
            const html = await new Promise((resolve, reject) => {
                ejs.renderFile(path.join(__dirname, 'template.ejs'), {
                    rows: filteredRows,
                    columns,
                    keys,
                    table,
                    title,
                    filesInvolved,
                    filtersApplied,
                    dateTimeString,
                    Descripcion,
                    Modulo,
                    registrosUnificados,
                    registrosDiferenciaP7P9,
                    porcentajeDiferenciaP7P9,
                    registrosDiferenciaP7PRD,
                    porcentajeDiferenciaP7PRD,
                    registrosDiferenciaPRDP9,
                    porcentajeDiferenciaPRDP9,
                    registrosConCualquierDiferencia,
                    porcentajeRegistrosConCualquierDiferencia
                }, (err, html) => {
                    if (err) {
                        reject(new Error('Error al renderizar el archivo EJS: ' + err.message));
                    } else {
                        resolve(html);
                    }
                });
            });

            fs.writeFileSync(outputPath, html, 'utf8');
            console.log(`El reporte se ha generado y guardado en ${outputPath}`);
        } catch (error) {
            throw new Error('Error al generar el reporte: ' + error.message);
        }

    } catch (error) {
        throw new Error('Error al generar el reporte: ' + error.message);
    }
}

module.exports = generateReport;


