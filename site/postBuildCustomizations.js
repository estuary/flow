const cheerio = require('cheerio');
const fs = require('fs');

const outputDir = './build';
const capConnectorsDir = `${outputDir}/reference/Connectors/capture-connectors`;
const matConnectorsDir = `${outputDir}/reference/Connectors/materialization-connectors`;
const conceptsDir = `${outputDir}/concepts`;
const connector = 'Connector';
const concept = 'Concept';
const divider = ' | ';

const updatePageTitles = (params, titleAddition) => {
    console.log(`Customizing ${params} BEGIN`);

    let updateCount = 0;
    fs.readdirSync(params, {
        recursive: true,
    }).forEach(file => {

        if (file.includes('.html')) {
            const fileFullPath = `${params}/${file}`;
            const $cheer = cheerio.load(fs.readFileSync(fileFullPath));
            const $title = $cheer("title")
            const titleText = $title.text();

            if (
                // Skip if it is already there (whether at the beginning, in plural, etc)
                !titleText.toLowerCase().includes(titleAddition.toLowerCase())
            ) {
                // Add 'Capture' or 'Materialization' to the title for connector ref pages
                if (params.includes('capture-connectors')) {
                    titleUpdate = `Capture ${titleAddition}`;
                } else if (params.includes('materialization-connectors')) {
                    titleUpdate = `Materialization ${titleAddition}`;
                } else {
                    titleUpdate = titleAddition;
                }

                const newTitle = titleText.replace(divider, ` ${titleUpdate}${divider}`);
                console.debug(`  - updating    : ${fileFullPath}`)
                console.debug(`    - new title : ${newTitle}`)

                $title.text(newTitle);
                fs.writeFileSync(fileFullPath, $cheer.html());
                updateCount += 1;
            }
        }
    });
    console.log(` - updated ${updateCount}`)
    console.log('Customizing DONE')

}

updatePageTitles(capConnectorsDir, connector);
updatePageTitles(matConnectorsDir, connector)
updatePageTitles(conceptsDir, concept);
