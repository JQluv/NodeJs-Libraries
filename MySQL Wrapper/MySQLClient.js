/**
 * Created by Geramy on 7/23/2016.
 */
mysql = require("mysql");
async = require("async");
//Private
var $dbhost = 'localhost';
var $dbusername = '';
var $dbpassword = '';
var $dbdatabase = '';
var MaxPending = 5;
//Public
module.exports = MySQLClient;

function MySQLClient() {
    var $connections = Array();
    var $sqlclient = this;
    this.QueriesRunning = [];
    this.Query = function(query, params, query_callback) {
        var FoundEngine = false;
        var connection = null;
        var total_pending = 0;
        async.each($connections,
            function(item, callback){
                if(item["count"] < 2 && item["usable"]) {
                    FoundEngine = true;
                    connection = item;
                }
                else if(item["count"] <= 0 && item["usable"]) {
                    total_pending += 1;
                    if(total_pending > MaxPending) {
                        item["useable"] = false;
                        item["conn"].end();
                        $connections = remove_array_value($connections, item);
                    }
                }
                callback();
            },
            function(err) {
                if(FoundEngine == false) {
                    item = CreateConnection(function(item) {
                        //new connection
                        connection = item;
                        connection.count += 1;
                        connection.conn.query(query, params, function(err, rows, fields) {
                            if(err == null) {
                                connection["count"] -= 1;
                                query_callback(rows, fields);
                            }
                            else {
                                try {
                                    item["useable"] = false;
                                    item["conn"].end();
                                    $connections = remove_array_value($connections, item);
                                    $sqlclient.Query(query, params, query_callback);
                                }
                                catch(err) {
                                    Console.log(err);
                                }
                            }
                        });
                    });
                }
                else if(FoundEngine == true) {
                    connection.count += 1;
                    connection.conn.query(query, params, function(err, rows, fields) {
                        if(err == null) {
                            connection["count"] -= 1;
                            query_callback(rows, fields);
                        }
                        else {
                            try {
                                connection["useable"] = false;
                                connection["conn"].end();
                                $connections = remove_array_value($connections, connection);
                                $sqlclient.Query(query, params, query_callback);
                            }
                            catch(err) {
                                console.log(err);
                            }
                        }
                    });
                }
            }
        );
    }

    var CreateConnection = function(itemcallback) {
        var con = mysql.createConnection({
            host: $dbhost,
            user: $dbusername,
            password: $dbpassword,
            database: $dbdatabase
        });
        con.connect(function(err){
            if(err == null) {
                connection = {"conn": con, "count": 0, "usable": true};
                $connections.push(connection);
                itemcallback(connection);
            }
            else
                itemcallback(null);
        });
        con.on('error', function(err) {
            console.log('db error');
            if(err.code === 'PROTOCOL_CONNECTION_LOST') { // Connection to the MySQL server is usually
                console.log("Disconnected from server, lost connection.");
            } else {                                      // connnection idle timeout (the wait_timeout
            }
        });
    }
    function remove_array_value(array, value) {
        var index = array.indexOf(value);
        if (index >= 0) {
            array.splice(index, 1);
            return reindex_array(array);
        }
    }
    function reindex_array(array) {
        var result = [];
        for (var key in array) {
            result.push(array[key]);
        }
        return result;
    }
}