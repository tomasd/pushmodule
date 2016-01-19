function Generator(server, reporter) {

}
Generator.prototype.onConnect = function (client, done) {
    client.send('sub /overview');
    client.
    setTimeout(done, 5000);
    //done();
};
module.exports = Generator
//module.exports = {
//    /**
//     * Before connection (optional, just for faye)
//     * @param {client} client connection
//     */
//    beforeConnect: function (client) {
//        // Do something
//    },
//
//    /**
//     * On client connection (required)
//     * @param {client} client connection
//     * @param {done} callback function(err) {}
//     */
//    onConnect: function (client, done) {
//        // client.send('Sailing the seas of cheese');
//        client.send('sub /overview');
//
//        done();
//    }
//};