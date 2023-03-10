const avro = require('avsc');

const type = avro.Type.forSchema({
  type: 'record',
  name: 'Person',
  fields: [
    {name: 'firstName', type: 'string'},
    {name: 'lastName', type: 'string'},
    {name: 'timestamp', type: 'string'},
    {name: 'age', type: 'int'}
  ]
});

module.exports = type;
// export default type;