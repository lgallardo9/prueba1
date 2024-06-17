const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

// Rutas de los archivos CSV que contienen los datos
const csvFilePath = path.join(__dirname, 'conf', 'az7db.csv');
const csvKeysFilePath = path.join(__dirname, 'conf', 'az7dbkeys.csv');
const csvColumnsFilePath = path.join(__dirname, 'conf', 'az7dbcolumns.csv');

// Función para cargar los datos desde el archivo CSV y retornarlos en formato JSON
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

// Función para buscar por Librería y Tabla
async function buscarTablaPorLibreriaYTabla(libreria, tabla) {
    try {
        const datos = await cargarDatos(csvFilePath);

        const tablaEncontrada = datos.find((fila) => fila.Libreria === libreria && fila.Tabla === tabla);
        if (tablaEncontrada) {
            return {
                Descripcion: tablaEncontrada.Descripcion,
                Modulo: tablaEncontrada.Modulo
            };
        } else {
            return null;
        }
    } catch (error) {
        console.error('Error al buscar la tabla:', error);
        return null;
    }
}

// Nueva función para buscar las keys por Librería y Tabla
async function buscarKeysPorLibreriaYTabla(libreria, tabla) {
    try {
        const datos = await cargarDatos(csvKeysFilePath);

        const keys = datos
            .filter((fila) => fila.Libreria === libreria && fila.Objeto === tabla)
            .sort((a, b) => parseInt(a['Numero campo clave']) - parseInt(b['Numero campo clave']))
            .map((fila) => fila['Nombre campo clave']);

        return keys;  // Retornar el array de valores de "Nombre campo clave"
    } catch (error) {
        console.error('Error al buscar las keys:', error);
        return null;
    }
}

// Nueva función para buscar las columnas por Librería y Tabla
async function buscarColumnasPorLibreriaYTabla(libreria, tabla) {
    try {
        const datos = await cargarDatos(csvColumnsFilePath);

        const columns = datos
            .filter((fila) => fila.Libreria === libreria && fila.Tabla === tabla)
            .map((fila) => fila['Campo']);

        return columns;  // Retornar el array de valores de "Campo"
    } catch (error) {
        console.error('Error al buscar las columnas:', error);
        return null;
    }
}

// Exportar las funciones para ser utilizadas desde otros archivos Node.js
module.exports = {
    buscarTablaPorLibreriaYTabla,
    buscarKeysPorLibreriaYTabla,
    buscarColumnasPorLibreriaYTabla
};
