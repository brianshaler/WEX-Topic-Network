###
# links.coffee
# Walks through entire articles data set and link related articles
###

fs = require "fs"
cluster = require "cluster"
pg = require "pg"
etree = require "elementtree"
max_threads = require("os").cpus().length

args  = require("optimist")
  .default("threads", max_threads)
  .default("host", "localhost")
  .default("port", "")
  .default("dbuser", "")
  .default("dbpassword", "")
  .default("db", "wex")
  .argv

args.threads = parseInt args.threads
threads = if args.threads < max_threads then args.threads else max_threads

db = null

get_connection_string = () ->
  protocol = "tcp://"
  creds = args.dbuser
  if creds != ""
    creds += if args.dbpassword == "" then creds + ":" + args.dbpassword else ""
    creds += if creds != "" then "@" : ""
  host = args.host
  dbname = args.db
  
  return protocol + creds + host + "/" + dbname

workers = []
workers_by_pid = {}

setup = () ->
  connection_string = get_connection_string()
  db = new pg.Client connection_string
  db.connect()
  
  if cluster.isMaster
    # Setup MASTER-specific stuff
    
    console.log "Connecting to #{connection_string}"
    
    db.query "DROP TABLE IF EXISTS links"
    # "IF NOT EXISTS" seems to be new to postgres as of v9.1
    db.query "CREATE TABLE IF NOT EXISTS links (source integer, target integer)", (err, result) ->
      for i in [0..threads]
        worker = cluster.fork()
        worker.on "message", worker_response
        workers_by_pid[worker.process.pid] = worker
        workers.push worker
      
      cluster.on "exit", (worker, code, signal) ->
        console.log "OH SHIT! Worker #" + worker.process.pid + " died"
      
      start()
    db.query "ALTER TABLE links ADD CONSTRAINT source_target UNIQUE (source, target)"
  else if cluster.isWorker
    # Setup WORKER-specific stuff
    process.on "message", master_request

# Ready to go, give each Worker a row
start = () ->
  for worker in workers
    do (worker) =>
      console.log "Starting worker #{worker.process.pid}"
      next_row worker.process.pid

# Currently only running 10 test iterations
offset = 0
max_offset = 1000
next_row = (last_pid) ->
  if offset < max_offset
    dispatch_message offset, last_pid
    offset++
  else
    # NOTE: This would immediately end the process with [threads-1] threads still working..
    wrap_up()

# Master process dispatching a message to the appropriate Worker
next_worker = 0
dispatch_message = (msg, pid) ->
  # sends a message/command to a specific Worker process, or the next one
  unless cluster.isMaster
    return
  targ = workers[0]
  if pid? and workers_by_pid[pid]?
    targ = workers_by_pid[pid]
  else
    targ = workers[next_worker]
    next_worker++
    if next_worker >= workers.length
      next_worker = 0
  
  #console.log "Message to "+targ.process.pid+": "+msg
  #console.log targ
  targ.send msg

# Master is receiving a confirmation of completion from a Worker thread
worker_response = (response) ->
  msg = response[0]
  pid = response[1]
  #console.log "Response from "+pid+": "+msg
  next_row pid

# Worker is receiving a command from the Master (msg is incremented offset)
master_request = (msg) ->
  db.query "SELECT wpid, name, xml FROM articles LIMIT 1 OFFSET $1", [msg], (err, result) =>
    row = result.rows[0]
    wpid = row.wpid
    xml = row.xml
    
    # TODO:
    # We can find related (potential parent or child topics) here, and fill up "targets" with `wpid`s to link
    data = etree.parse xml
    targets = data.findall("*/paragraph")[0].findall "sentence//target"
    if targets.length == 0
      if data.findall("*/paragraph")[1]
        targets = data.findall("*/paragraph")[1].findall "sentence//target"
      else
        targets = data.findall("*/paragraph//target")
    
    #console.log targets
    
    target_articles = []
    for t in targets
      do (t) =>
        target_articles.push "name LIKE '" + t.text.replace(/'/g,"%") + "'"
    
    if target_articles.length == 0
      console.log "NO LINKS?? #{wpid}"
      process.send ["done", process.pid]
    else
      like_string = target_articles.join " OR "
      console.log "SELECT wpid FROM articles WHERE #{like_string}"
      db.query "SELECT wpid FROM articles WHERE #{like_string};", (err, result) =>
        if err
          throw err
        
        console.log "Results: #{result.rows.length}"
        inserted = 0
        for row in result.rows
          do (row) =>
            if row.wpid != wpid
              inserted++
              db.query {text: "INSERT INTO links (source, target) values($1, $2);", values: [wpid, row.wpid]}, (err, result) =>
                if err
                  throw err
                
                console.log "Child process #{process.pid} inserted link: #{wpid} => #{row.wpid}"
        
        if inserted == 0
          console.log "None inserted out of #{target_articles.length}"
        
        process.send ["done", process.pid]
  

# Currently stops immediately, although some Worker threads my not be done yet...
wrap_up = () ->
  console.log "All done!"
  setTimeout () =>
    process.exit()
  , 1000

# Everything is defined... GO!
setup()
