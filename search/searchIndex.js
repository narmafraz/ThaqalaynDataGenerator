const { Document } = require("flexsearch");
const fs = require("fs");
const path = require("path");

// Path to the exported index file
const indexPath = path.join(__dirname, 'index.json');

// Initialize FlexSearch index
const index = new Document({
    tokenize: "forward",
    threshold: 0.3,
    document: {
        id: "index",
        index: ["crumbs", "titles", "text", "narrator_chain", "translations"]
    }
});

// Function to import the index from the file
const importIndex = async () => {
    const indexData = JSON.parse(fs.readFileSync(indexPath, 'utf8'));

    await Promise.all(Object.keys(indexData).map(key => {
        return index.import(key, indexData[key]);
    }));

    console.log('Index has been imported successfully.');
};

// Function to perform a search
const search = async (query) => {
    const results = await index.search(query, { limit: 5 });
    console.log("Search results for:", query);
    console.log(results);
};

// Run the import and search functions
const run = async () => {
    await importIndex();
    await search("رَسُولَ");
};

run();
