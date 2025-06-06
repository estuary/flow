// Need to use the browser version -> https://github.com/mjmlio/mjml/issues/2447
import mjml2Html from "npm:mjml-browser@4.14.1";

export interface Recipient {
    email: string;
    full_name: string | null;
}

export const commonTemplate = (body: string, recipient: Recipient | null) => {
    // We could also fall back to 'Dear dave@estuary.dev', but that might look weirdly spammy
    const dearLine = recipient?.full_name ? `<mj-text font-size="20px" color="#512d0b"><strong>Dear ${recipient.full_name},</strong></mj-text>` : "";
    const mjml = `
      <mjml>
        <mj-head>
          <mj-attributes>
            <mj-all padding="0px"></mj-all>
            <mj-text font-family="Ubuntu, Helvetica, Arial, sans-serif" font-size="17px" padding-bottom="10px" line-height="1.4"></mj-text>
            <mj-button font-family="Ubuntu, Helvetica, Arial, sans-serif" background-color="#5072EB" color="white" padding="25px 0 0 0" font-weight="400" font-size="17px"></mj-button>
          </mj-attributes>
          <mj-style inline="inline">
            a { text-decoration: none!important; }
            .identifier {
              background-color: #dadada;
              padding: 2px 3px;
              border-radius: 2px;
              font-family: monospace;
              font-weight: bold;
            }
          </mj-style>
        </mj-head>
        <mj-body>
          <mj-section>
            <mj-column width="100%">
              <mj-image
                src="https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//estuary_logo_comfy_14513ca434/estuary_logo_comfy_14513ca434.jpg"
                alt="Estuary Logo"
                padding="20px 0 20px 0"
              />
            </mj-column>
          </mj-section>
          <mj-section padding-bottom="20px">
            <mj-column>
              <mj-divider border-width="1px" border-style="dashed" border-color="grey" />
            </mj-column>
          </mj-section>
          <mj-section>
            <mj-column>
              ${dearLine}
              ${body}
            </mj-column>
          </mj-section>
          <mj-section>
            <mj-column>
              <mj-text color="#000000" font-size="14px" font-family="Arial, sans-serif" padding-top="15px">Thanks, <br /> Estuary Team
              </mj-text>
            </mj-column>
          </mj-section>
        </mj-body>
      </mjml>
      `;
    console.log(mjml);
    return mjml2Html(mjml).html;
};
