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
            const $cheer = cheerio.load(fs.readFileSync(`${connectorsDir}/${file}`));
            const $title = $cheer("title")
            const titleText = $title.text();

            if (!titleText.includes(titleAddition)) {
                const fileFullPath = `${connectorsDir}/${file}`;
                console.log('-updating', {
                    path: fileFullPath
                })

                const newTitle = titleText.replace(divider, titleAddition);

                $title.text(newTitle);

                console.log(`-title`, {
                    newTitle
                })

                fs.writeFileSync(fileFullPath, $cheer.html());
            }
        }
    });
    console.log('Customizing DONE')

}

updateAllConnectorPages(connectorsDir, `Connector${divider}`);
