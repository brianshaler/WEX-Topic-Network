WEX-Topic-Network
===================

Tools to process the Freebase Wikipedia Extraction (WEX) data dump.

To get started, install dependencies:

    npm install

Then, you can either run links.js (All parameters are optional)

    node links.js [--host localhost] [--port 1234] [--dbuser postgres] [--dbpassword mypassword] [--db wex] [--threads 2]

Or, if you've changed the CoffeeScript source file, compile and run:

    coffee -c links.coffee && node links.js

