const cheerio = require('cheerio');
const fs = require('fs');

const outputDir = './build';
const connectorsDir = `${outputDir}/reference/Connectors`;
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
                // Skip if we are on a specific "root" page
                !titleText.toLowerCase().startsWith('dekaf integrations'.toLowerCase()) &&
                !titleText.toLowerCase().startsWith('materialization protocol'.toLowerCase()) &&

                // Skip if it is already there (whether at the beginning, in plural, etc)
                !titleText.toLowerCase().includes(titleAddition.toLowerCase())
            ) {
                const newTitle = titleText.replace(divider, ` ${titleAddition}${divider}`);
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

updatePageTitles(connectorsDir, `${connector}`);
updatePageTitles(conceptsDir, `${concept}`);
