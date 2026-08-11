import React from 'react';
import clsx from 'clsx';
import Translate from '@docusaurus/Translate';
import AdmonitionLayout from '@theme/Admonition/Layout';
import IconWarning from '@theme/Admonition/Icon/Warning';

// Renders identically to `:::warning`, but under its own admonition type
// (`theme-admonition-deprecated`, applied automatically by AdmonitionLayout)
// so it can be targeted independently — e.g. by Kapa's search crawler to
// exclude deprecated connector pages from search results.
const infimaClassName = 'alert alert--warning';
const defaultProps = {
  icon: <IconWarning />,
  title: (
    <Translate
      id="theme.admonition.deprecated"
      description="The default label used for the Deprecated admonition (:::deprecated)">
      deprecated
    </Translate>
  ),
};

export default function AdmonitionTypeDeprecated(props) {
  return (
    <AdmonitionLayout
      {...defaultProps}
      {...props}
      className={clsx(infimaClassName, props.className)}>
      {props.children}
    </AdmonitionLayout>
  );
}
