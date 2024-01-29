import mjml2Html from "https://esm.sh/mjml-browser@4.14.1?dts";

export const commonTemplate = (body: string) =>
    mjml2Html(`
<mjml>
  <mj-head>
    <mj-attributes>
      <mj-all padding="0px"></mj-all>
      <mj-text font-family="Ubuntu, Helvetica, Arial, sans-serif" font-size="13px" padding-bottom="10px"></mj-text>
    </mj-attributes>
    <mj-style inline="inline">
      a { text-decoration: none!important; }
      .identifier {
        background-color: #dadada;
        padding: 1px 6px;
        border-radius: 2px;
        font-family: monospace;
      }
    </mj-style>
  </mj-head>
  <mj-body>
    <mj-section>
      <mj-column width="100%">
        <mj-image src="https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//estuary_logo_071fa2dcfb/estuary_logo_071fa2dcfb.png" width="300px" alt="header image" padding="20px"></mj-image>
      </mj-column>
    </mj-section>
    <mj-section padding-bottom="20px">
      <mj-column>
        <mj-divider border-width="2px" />
      </mj-column>
    </mj-section>
    <mj-section>
      <mj-column padding-left="10px">
        ${body}
      </mj-column>
    </mj-section>
    <mj-section>
      <mj-column>
        <mj-text color="#000000" font-size="14px" font-family="Arial, sans-serif" padding-top="40px">Thanks, <br /> Estuary Team
        </mj-text>
      </mj-column>
    </mj-section>
  </mj-body>
</mjml>
`);
