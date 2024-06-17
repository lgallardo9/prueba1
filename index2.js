const fs = require('fs');
const csv = require('csv-parser');
const ejs = require('ejs');
const path = require('path');
const { buscarTablaPorLibreriaYTabla, buscarKeysPorLibreriaYTabla, buscarColumnasPorLibreriaYTabla } = require('./buscarTabla');
const processFiles = require('./processFiles');
const generateReport = require('./generateReport');
const generateSQLScript = require('./generateSQLScript');
const csvCompFilePath = path.join(__dirname, 'conf', 'az7comp.csv');

const channels = ['P7', 'P9', 'PRD'];
let data = {};
let combinedData = {};
let globalUniqueKeys = new Set(); // Conjunto global para rastrear claves únicas

// Verificar si los parámetros opcionales están presentes
const onlyChanges = process.argv.includes('--only-changes');
const onlyPRD = process.argv.includes('--only-prd');

async function cargarDatos(filePath) {
    const datos = [];

    try {
        const stream = fs.createReadStream(filePath)
            .pipe(csv());

        for await (const row of stream) {
            datos.push(row);
        }

        return datos;
    } catch (error) {
        throw new Error('Error al cargar los datos desde el archivo CSV: ' + error.message);
    }
}

async function main() {
    try {
        const filas = await cargarDatos(csvCompFilePath);

        for (let i = 0; i < filas.length; i++) {
            const { Libreria: library, Tabla: table, P7, P9, PRD } = filas[i];

            const resultadoBusqueda = await buscarTablaPorLibreriaYTabla(library, table);
            if (!resultadoBusqueda) {
                console.error(`No se encontró la tabla con Librería "${library}" y Tabla "${table}".`);
                process.exit(1);
            }

            console.log(`Descripción: ${resultadoBusqueda.Descripcion}`);
            console.log(`Módulo: ${resultadoBusqueda.Modulo}`);

            const Descripcion = `Descripción: ${resultadoBusqueda.Descripcion}`;
            const Modulo = `Módulo: ${resultadoBusqueda.Modulo}`;
            const countryMatch = PRD.match(/PRD_(\w+)_/);

            if (!countryMatch) {
                console.error(`No se pudo determinar el país del archivo ${PRD}.`);
                continue;
            }

            const country = countryMatch[1]; // Obtener el país del PRD

            let columns, keys, combinedData, globalUniqueKeys;

            try {
                columns = await buscarColumnasPorLibreriaYTabla(library, table);
                keys = await buscarKeysPorLibreriaYTabla(library, table);
                const { combinedData: processData, globalUniqueKeys: processKeys } = await processFiles(library, table, country, columns, keys);
                combinedData = processData;
                globalUniqueKeys = processKeys;
            } catch (error) {
                console.error(`Error al buscar columnas, keys o procesar archivos para la línea ${i + 2} del archivo az7comp.csv:`, error);
                process.exit(1);
            }

            if (!columns || !Array.isArray(columns) || columns.length === 0 ||
                !keys || !Array.isArray(keys) || keys.length === 0) {
                console.error(`No se encontraron columnas o keys válidos para la librería "${library}" y tabla "${table}" en la línea ${i + 2} del archivo az7comp.csv.`);
                process.exit(1);
            }

            await generateReport(library, table, country, Descripcion, Modulo, columns, keys, combinedData, globalUniqueKeys, onlyChanges, onlyPRD);

            if (typeof generateSQLScript === 'function') {
                await generateSQLScript(library, table, columns, keys, combinedData, onlyPRD);
            }
        }

        console.log('Proceso completado exitosamente.');
    } catch (error) {
        console.error('Error al ejecutar el programa:', error);
        process.exit(1);
    }
}


main();
