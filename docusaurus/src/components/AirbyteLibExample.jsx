import React from "react";
import { JSONSchemaFaker } from "json-schema-faker";
import CodeBlock from '@theme/CodeBlock';
import Heading from '@theme/Heading';


export const AirbyteLibExample = ({
  specJSON,
  connector,
  withHeading,
}) => {
  const spec = JSON.parse(specJSON);
  const fakeConfig = JSONSchemaFaker.generate(spec);
  return <>
    {withHeading && <Heading as="h2" id="usage-with-airbyte-lib">Usage with airbyte-lib</Heading>}
    <p>
      Install the Python library via:
    </p>
    <CodeBlock
        language="bash">{"pip install airbyte-lib"}</CodeBlock>
    <p>Then, execute a sync by loading the connector like this:</p>
    <CodeBlock
      language="python"
    >{`import airbyte_lib as ab

config = ${JSON.stringify(fakeConfig, null, 2)}

result = ab.get_connector(
    "${connector}",
    config=config,
).read()

for record in result.cache.streams["my_stream:name"]:
  print(record)`} </CodeBlock>
    <p>You can find more information in the airbyte_lib quickstart guide.</p>
  </>;
};
