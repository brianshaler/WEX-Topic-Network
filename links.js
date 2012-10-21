// Generated by CoffeeScript 1.3.3

/*
# links.coffee
# Walks through entire articles data set and link related articles
*/


(function() {
  var args, cluster, db, dispatch_message, fs, get_connection_string, master_request, max_offset, max_threads, next_row, next_worker, offset, pg, setup, start, threads, worker_response, workers, workers_by_pid, wrap_up;

  fs = require("fs");

  cluster = require("cluster");

  pg = require("pg");

  max_threads = require("os").cpus().length;

  args = require("optimist")["default"]("threads", max_threads)["default"]("host", "localhost")["default"]("port", "")["default"]("dbuser", "")["default"]("dbpassword", "")["default"]("db", "wex").argv;

  args.threads = parseInt(args.threads);

  threads = args.threads < max_threads ? args.threads : max_threads;

  db = null;

  get_connection_string = function() {
    var creds, dbname, host, protocol;
    protocol = "tcp://";
    creds = args.dbuser;
    if (creds !== "") {
      creds += args.dbpassword === "" ? creds + ":" + args.dbpassword : "";
      creds += creds !== "" ? {
        "@": ""
      } : void 0;
    }
    host = args.host;
    dbname = args.db;
    return protocol + creds + host + "/" + dbname;
  };

  workers = [];

  workers_by_pid = {};

  setup = function() {
    var connection_string;
    connection_string = get_connection_string();
    db = new pg.Client(connection_string);
    db.connect();
    if (cluster.isMaster) {
      console.log("Connecting to " + connection_string);
      return db.query("CREATE TABLE IF NOT EXISTS links (source integer, target integer)", function(err, result) {
        var i, worker, _i;
        for (i = _i = 0; 0 <= threads ? _i <= threads : _i >= threads; i = 0 <= threads ? ++_i : --_i) {
          worker = cluster.fork();
          worker.on("message", worker_response);
          workers_by_pid[worker.process.pid] = worker;
          workers.push(worker);
        }
        cluster.on("exit", function(worker, code, signal) {
          return console.log("OH SHIT! Worker #" + worker.process.pid + " died");
        });
        return start();
      });
    } else if (cluster.isWorker) {
      return process.on("message", master_request);
    }
  };

  start = function() {
    var worker, _i, _len, _results,
      _this = this;
    _results = [];
    for (_i = 0, _len = workers.length; _i < _len; _i++) {
      worker = workers[_i];
      _results.push((function(worker) {
        console.log("Starting worker " + worker.process.pid);
        return next_row(worker.process.pid);
      })(worker));
    }
    return _results;
  };

  offset = 0;

  max_offset = 20;

  next_row = function(last_pid) {
    if (offset < max_offset) {
      dispatch_message(offset, last_pid);
      return offset++;
    } else {
      return wrap_up();
    }
  };

  next_worker = 0;

  dispatch_message = function(msg, pid) {
    var targ;
    if (!cluster.isMaster) {
      return;
    }
    targ = workers[0];
    if ((pid != null) && (workers_by_pid[pid] != null)) {
      targ = workers_by_pid[pid];
    } else {
      targ = workers[next_worker];
      next_worker++;
      if (next_worker >= workers.length) {
        next_worker = 0;
      }
    }
    return targ.send(msg);
  };

  worker_response = function(response) {
    var msg, pid;
    msg = response[0];
    pid = response[1];
    return next_row(pid);
  };

  master_request = function(msg) {
    var _this = this;
    return db.query("SELECT wpid FROM articles LIMIT 1 OFFSET $1", [msg], function(err, result) {
      var target, targets, wpid, _fn, _i, _len;
      wpid = result.rows[0].wpid;
      targets = [0, 1];
      _fn = function(target) {
        return db.query({
          text: "INSERT INTO links (source, target) values($1, $2)",
          values: [wpid, target]
        }, function(err, result) {
          return console.log("Child process " + process.pid + " inserted link: " + wpid + " => " + target);
        });
      };
      for (_i = 0, _len = targets.length; _i < _len; _i++) {
        target = targets[_i];
        _fn(target);
      }
      return process.send(["done", process.pid]);
    });
  };

  wrap_up = function() {
    console.log("All done!");
    return process.exit();
  };

  setup();

}).call(this);