const path = require("path");
const fs = require("fs");
const FlexSearch = require('flexsearch');

// Create a new FlexSearch index
const index = new FlexSearch.Document({
    tokenize: "forward",
    cache: true,
    document: {
        id: "id",
        index: [
            "title",
            "content"
        ]
    }
});

// Example data to index
const documents = [
    { id: 1, title: "Node.js Introduction", content: "Node.js is a JavaScript runtime built on Chrome's V8 JavaScript engine." },
    { id: 2, title: "JavaScript Basics", content: "JavaScript is a programming language that conforms to the ECMAScript specification." },
    { id: 3, title: "FlexSearch Guide", content: "FlexSearch is a full-text search library for JavaScript." },
    { id: 4, title: "Advanced JavaScript", content: "Learn about advanced concepts in JavaScript." }
];

// Index the documents
documents.forEach(doc => {
    index.add(doc);
});

// Save the index to a file
const exportIndex = async () => {
    const exportedIndex = {};

    await index.export((key, data) => {
        exportedIndex[key] = data;
    });

    fs.writeFileSync(path.join(__dirname, 'exampleIndex.json'), JSON.stringify(exportedIndex));
    console.log('Index has been created and saved to exampleIndex.json');
};

// Function to perform a search
const search = async (query) => {
    const results = await index.search(query, { limit: 5 });
    console.log("Search results for:", query);
    console.log(results);
};

// Run the export and search functions
const run = async () => {
    await exportIndex();
    await search("JavaScript");
};

run();
