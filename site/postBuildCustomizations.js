const cheerio = require('cheerio');
const fs = require('fs');

const outputDir = './build';
const connectorsDir = `${outputDir}/reference/Connectors`
const connector = 'Connector';
const divider = ' | ';

const updateAllConnectorPages = (params, titleAddition) => {
    console.log('Customizing BEGIN')

    let updateCount = 0;
    fs.readdirSync(params, {
        recursive: true,
    }).forEach(file => {

        if (file.includes('.html')) {
            const fileFullPath = `${connectorsDir}/${file}`;
            const $cheer = cheerio.load(fs.readFileSync(fileFullPath));
            const $title = $cheer("title")
            const titleText = $title.text();

            if (!titleText.startsWith('Connector') &&
                !titleText.startsWith('Dekaf integrations') && 
                !titleText.includes(titleAddition) && 
                !titleText.includes('connectors |')
            ) {
                console.debug(`    -updating ${fileFullPath}`)

                $title.text(titleText.replace(divider, titleAddition));
                fs.writeFileSync(fileFullPath, $cheer.html());
                updateCount += 1;
            }
        }
    });
    console.log(` - updated ${updateCount}`)
    console.log('Customizing DONE')

}

updateAllConnectorPages(connectorsDir, ` ${connector}${divider}`);
