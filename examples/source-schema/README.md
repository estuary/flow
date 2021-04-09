# Transform Source Schemas

Example of using a transform source schema to apply a more strict validation to the documents in a
collection with a more permissive schema. This pattern allows you to be very permissive in capturing
data from an external source, and defer validation until the derivation. The permissive capture will
continue to accept data, but the restrictive derivation will error and stop processing when the
first invalid document is encountered. You can then update the derivation to account for the invalid
document, and it will catch back up from the historical data in the permissive collection.

## Example

We expect that documents captured by the `permissive` collection will look like 
`{"id": "foo", "a": 1, "b": true, "c": 5.2}`. If any document doesn't match that, we still want to
capture it, though, and just adjust our derivation so that it can cope with the new shape of the
data. So say we capture the document `{"id": "foo", "a": "a string", "b": false, "c": 8.2}` into the
`permissive` collection. When the `restrictive` derivation reads that document, it's going to fail
with a validation error, since the source schema used in the derivation specifies that `a` must be
an `integer`. This causes the `restrictive` derivation to halt processing.

Then we update the `restrictive` derivation to handle the string data in `a`. First we relax the
source schema to have `a: {type: [integer, string]}`. Then we update the code to do something
sensible with the strings. Finally the updated derivation is applied by running `flowctl apply`, and
it resumes processing right where it left off.

