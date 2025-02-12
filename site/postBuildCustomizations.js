const cheerio = require('cheerio');
const fs = require('fs');

const outputDir = './build';
const connectorsDir = `${outputDir}/reference/Connectors`
const divider = ' | ';

const updateAllConnectorPages = (params, titleAddition) => {
    console.log('Customizing BEGIN')

    fs.readdirSync(params, {
        recursive: true,
    }).forEach(file => {

        if (file.includes('.html')) {
            const fileFullPath = `${connectorsDir}/${file}`;
            const $cheer = cheerio.load(fs.readFileSync(fileFullPath));
            const $title = $cheer("title")

            if (!$title.text().includes(titleAddition)) {
                console.log('-updating', {
                    path: fileFullPath
                })

                $title.text(titleText.replace(divider, titleAddition));
                fs.writeFileSync(fileFullPath, $cheer.html());
            }
        }
    });
    console.log('Customizing DONE')

}

updateAllConnectorPages(connectorsDir, ` Connector${divider}`);
