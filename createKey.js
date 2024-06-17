async function createKey(row, keys) {
    const key = keys.map(key => row[key]).join('-');
    return key
}

module.exports = createKey;