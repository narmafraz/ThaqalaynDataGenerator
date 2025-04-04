const { Document } = require("flexsearch");
const fs = require("fs");
const path = require("path");

// Define the base directory
const baseDir = path.join(__dirname, '../../ThaqalaynData/books');

// Initialize FlexSearch index
const index = new Document({
    tokenize: "forward",
    threshold: 0.3,
    document: {
        id: "index",
        index: ["titles", "text", "narrator_chain", "translations"]
    }
});

// Function to read JSON files recursively
function readFiles(dir) {
    const files = fs.readdirSync(dir);
    files.forEach(file => {
        const fullPath = path.join(dir, file);
        const stat = fs.lstatSync(fullPath);

        if (stat.isDirectory()) {
            readFiles(fullPath);
        } else if (path.extname(fullPath) === '.json') {
            const data = JSON.parse(fs.readFileSync(fullPath, 'utf8'));
            processFile(data, fullPath);
        }
    });
}

// Function to process each JSON file
function processFile(data, filePath) {
    if (data.kind === 'verse_list') {
        const baseIndex = data.index;
        const titles = `${data.data.titles.en || ''} ${data.data.titles.ar || ''}`.trim();

        data.data.verses.forEach(verse => {
            const verseText = verse.text ? verse.text.join(' ') : '';
            const narratorChain = verse.narrator_chain ? verse.narrator_chain.text : '';
            const translations = verse.translations ? Object.values(verse.translations).flat().join(' ') : '';

            const entry = {
                index: `${baseIndex}:${verse.local_index}`,
                titles: titles,
                text: verseText,
                narrator_chain: narratorChain,
                translations: translations
            };

            // console.log(`Adding entry to index from file ${filePath}:`, entry);

            index.add(entry);
        });
    } else {
        console.log(`Skipping file ${filePath} because it does not contain verse_list kind.`);
    }
}

// Read and process files
readFiles(baseDir);



// Save the index to a file
const exportIndex = async () => {
    const exportedIndex = {};

    await index.export((key, data) => {
        exportedIndex[key] = data;
    });

    fs.writeFileSync(path.join(__dirname, 'index.json'), JSON.stringify(exportedIndex));
    console.log('Index has been created and saved to index.json');
};

// Run the export
const runExport = async () => {
    await exportIndex();
};

runExport();
