const fs = require('fs');
const path = require('path');

async function generateSQLScript(library, table, columns, keys, combinedData, onlyPRD) {
    const sqlCreateTableStatement = [];
    const sqlInsertStatementsOutput = [];
    const sqlInsertStatementsSelect = [];
    let isFirstInsert = true;

    Object.values(combinedData).forEach((row) => {
        const keysWhere = keys.map(key => `${key} = '${row[key]}'`).join(' AND ');

        if (onlyPRD && row.sources.PRD && !row.sources.P9) {
            const insertColumns = columns.map(column => `'${row[column] || ''}'`);
            const insertStatement = `INSERT INTO ${library}.${table} (${columns.join(', ')}) VALUES (${insertColumns.join(', ')});`;
            const insertStatementSelect = `INSERT INTO QGPL.TMP${table} (${columns.join(', ')}) VALUES (${insertColumns.join(', ')});`;

            if (isFirstInsert) {
                const selectStatement = `SELECT * FROM ${library}.${table} WHERE ${keysWhere}`;
                const selectStatementWithITLibrary = selectStatement.replace('QA', 'IT');

                const createTableStatement = `CREATE TABLE QGPL.TMP${table} AS (${selectStatementWithITLibrary}) WITH NO DATA;`;
                sqlCreateTableStatement.push(createTableStatement);
                isFirstInsert = false;
            }

            // Replace 'QA' with 'IT' in insert statements
            const insertStatementWithITLibrary = insertStatement.replace('QA', 'IT');
            const insertStatementSelectWithITLibrary = insertStatementSelect.replace('QA', 'IT');
            sqlInsertStatementsOutput.push(insertStatementWithITLibrary);
            sqlInsertStatementsSelect.push(insertStatementSelectWithITLibrary);

        } else if (row.sources.PRD && row.sources.P9 && !onlyPRD) {
            let updateNeeded = false;
            const updateColumns = [];

            columns.forEach(column => {
                if (!keys.includes(column) && row[column] !== row[column]) {
                    updateColumns.push(`${column} = '${row[column]}'`);
                    updateNeeded = true;
                }
            });

            if (updateNeeded) {
                const updateStatement = `UPDATE ${library}.${table} SET ${updateColumns.join(', ')} WHERE ${keysWhere};`;
                sqlInsertStatementsOutput.push(updateStatement);
            }
        }
    });

    if (sqlInsertStatementsSelect.length > 0) {
        const finalSelectStatement = `SELECT * FROM ${library}.${table} A, QGPL.TMP${table} B WHERE ${keys.map(key => `A.${key} = B.${key}`).join(' AND ')};`;
        const finalSelectStatementWithITLibrary = finalSelectStatement.replace('QA', 'IT');

        sqlInsertStatementsSelect.push(finalSelectStatementWithITLibrary);
    }

    const outputDir = path.join(__dirname, 'out');

    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir);
    }

    const outputSQLPath = path.join(outputDir, `${table}_sql.sql`);
    const selectSQLPath = path.join(outputDir, `it_QuerySelect_${table}.sql`);

    try {
        await Promise.all([
            fs.promises.writeFile(outputSQLPath, sqlInsertStatementsOutput.join('\n')),
            fs.promises.writeFile(selectSQLPath, [...sqlCreateTableStatement, ...sqlInsertStatementsSelect].join('\n'))
        ]);

        console.log(`Archivos SQL generados y guardados en ${outputDir}`);
    } catch (error) {
        throw new Error(`Error al escribir archivos SQL: ${error.message}`);
    }
}

module.exports = generateSQLScript;
