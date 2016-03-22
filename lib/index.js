//-------------------------------------------------------------------------------
// Copyright IBM Corp. 2015
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-------------------------------------------------------------------------------

'use strict';

var util = require('util');

var pipesSDK = require('simple-data-pipe-sdk');
var connectorExt = pipesSDK.connectorExt;

var bluemixHelperConfig = require('bluemix-helper-config');
var global = bluemixHelperConfig.global;

// This connector uses the passport strategy module (http://passportjs.org/) for reddit.
var dataSourcePassportStrategy = require('passport-reddit').Strategy; 

var request = require('request');
var _ = require('lodash');


/**
 * Sample connector that stores a few JSON records in Cloudant
 * Build your own connector by following the TODO instructions
 */
function oAuthRedditConnector(){

	 /* 
	  * Customization is mandatory
	  */

	// TODO: 
	//   Replace 'Reddit OAuth Data Source' with the desired display name of the data source (e.g. reddit) from which data will be loaded
	var connectorInfo = {
						  id: require('../package.json').simple_data_pipe.name,			// derive internal connector ID from package.json
						  name: 'Reddit OAuth Data Source'								// TODO; change connector display name
						};

	// TODO: customize options						
	var connectorOptions = {
					  		recreateTargetDb: true, // if set (default: false) all data currently stored in the staging database is removed prior to data load
					  		useCustomTables: true   // keep true (default: false)
						   };						

	// Call constructor from super class; 
	connectorExt.call(this, 
					 	connectorInfo.id, 			
					 	connectorInfo.name, 
					 	connectorOptions	  
					 );	

    // reddit API access requires a unique user-agent HTTP header; change this default (https://github.com/reddit/reddit/wiki/API)
	var userAgentHTTPHeaderValue = 'Simple Data Pipe demo application';

	// writes to the application's global log file
	var globalLog = this.globalLog;

	/*
	 * ---------------------------------------------------------------------------------------
	 * Override Passport-specific connector methods:
	 *  - getPassportAuthorizationParams
	 *  - getPassportStrategy
	 *  - passportAuthCallbackPostProcessing
	 * ---------------------------------------------------------------------------------------
	 */

	/**
	 * Returns a fully configured Passport strategy for reddit.
	 * @override
	 * @returns {duration:'permanent'} {@link https://github.com/reddit/reddit/wiki/OAuth2}
	 */
	this.getPassportAuthorizationParams = function() {
       return {duration:'permanent'};
	}; // getPassportAuthorizationParams

	/**
	 * Returns a fully configured Passport strategy for reddit. The passport verify
	 * callback adds two properties to the profile: oauth_access_token and oauth_refresh_token.
	 * @override
	 * @returns {Object} Passport strategy for reddit.
	 * @returns {Object} profile - user profile returned by reddit
	 * @returns {string} profile.oauth_access_token
	 * @returns {string} profile.oauth_refresh_token
	 */
	this.getPassportStrategy = function(pipe) {

		return new dataSourcePassportStrategy({
			clientID: pipe.clientId,											 // mandatory; oAuth client id; do not change
	        clientSecret: pipe.clientSecret,									 // mandatory; oAuth client secret;do not change
	        callbackURL: global.getHostUrl() + '/authCallback',		 			 // mandatory; oAuth callback; do not change
	        customHeaders: {'User-Agent': userAgentHTTPHeaderValue},             // reddit requires a unique user-agent HTTP header
	        scope: 'identity,read'												 // See https://www.reddit.com/dev/api/oauth for scope list
		  },
		  function(accessToken, refreshToken, profile, done) {					 

			  process.nextTick(function () {

			  	// attach the obtained access token to the user profile
		        profile.oauth_access_token = accessToken; 

			  	// attach the obtained refresh token to the user profile		        
		        profile.oauth_refresh_token = refreshToken; 

		        // return the augmented profile
			    return done(null, profile);
			  });
		  }
		);
	}; // getPassportStrategy

	/**
	 * Attach OAuth access token and OAuth refresh token to data pipe configuration.
	 * @param {Object} profile - the output returned by the passport verify callback
	 * @param {pipe} pipe - data pipe configuration, for which OAuth processing has been completed
	 * @param callback(err, pipe ) error information in case of a problem or the updated pipe
	 */
	this.passportAuthCallbackPostProcessing = function( profile, pipe, callback ){
				
		if((!profile) || (! profile.oauth_access_token) || (! profile.oauth_refresh_token)) {
			globalLog.error('Internal application error: OAuth parameter is missing in passportAuthCallbackPostProcessing');
			return callback('Internal application error: OAuth parameter is missing.'); 			
		}

		if(!pipe) {
			globalLog.error('Internal application error: data pipe configuration parameter is missing in passportAuthCallbackPostProcessing');
			return callback('Internal application error: data pipe configuration parameter is missing.'); 
		}

        // Attach the token(s) and other relevant information from the profile to the pipe configuration.
        // Use this information in the connector code to access the data source

		pipe.oAuth = { 
						accessToken : profile.oauth_access_token, 
						refreshToken: profile.oauth_refresh_token 
					};

		// Fetch list of data sets that the user can choose from; the list is displayed in the Web UI in the "Filter Data" panel.
        // Attach data set list to the pipe configuration
		this.getRedditDataSetList(pipe, function (err, pipe){
			if(err) {
		    	globalLog.error('OAuth post processing failed. The reddit data set list could not be created for data pipe configuration ' + pipe._id + ': ' + err);
		    }	
		    else {
			    globalLog.debug('OAuth post processing completed. Data pipe configuration was updated: ');
			    globalLog.debug(' ' + util.inspect(pipe,3));
		    }	

			return callback(err, pipe);
		});

	}; // passportAuthCallbackPostProcessing

	/*
	 * Customization is mandatory!
	 * @param {Object} pipe - Data pipe configuration
	 * @param {callback} done - invoke after processing is complete or has resulted in an error; parameters (err, updated_pipe)
	 * @return list of data sets (also referred to as tables for legacy reasons) from which the user can choose from
	 */
	this.getRedditDataSetList = function(pipe, done){

		// List of reddit data sets a user can choose from
		var dataSets = [];

		// sample request only; retrieve list of hot AMA topics
		var requestOptions = {
			            url : 'https://oauth.reddit.com/r/iAMA/hot?count=10',
			            headers: {
			            			'User-Agent' : userAgentHTTPHeaderValue,
			            			'Authorization' : 'bearer ' + pipe.oAuth.accessToken
			            }
			        };

		// submit request, e.g. https://oauth.reddit.com/r/iAMA/hot?count=10 (https://www.reddit.com/r/iAMA/hot.json?count=10) 
		request.get(requestOptions, function(err, response, body) {

	    		if(err) {
	    			// there was a problem with the request; abort processing
	    			// by calling the callback and passing along an error message
					return done('Fetch request err: ' + err, null); 
	    		}

	    		// process results
	    		var hotList = JSON.parse(body);	
	    		if(hotList.kind === 'Listing') {
	    			_.forEach(hotList.data.children, function (ama) {
		    			// 'Define' the data set or data sets that can be loaded from the data source. The user gets to choose one.
						// Sample: dataSets.push({name:'InternalRedditDataSetName', label:'DisplayedRedditDataSetName'});
						//         name = unique reddit identifier, e.g. 49jkhn 
						//         label = AMA title, e.g. "Hello Reddit, it's Sacha Baron Cohen, star of action ..." 
	    				dataSets.push({
	    								name: ama.data.id, 
	    					       		label: ama.data.title
	    					          });
	    			});
	    		}

				// TODO: If you want to provide the user with the option to load all data sets concurrently, define a single data set that 
				// contains only property 'labelPlural', with a custom display label: 
				//  dataSets.push({labelPlural:'All data sets'});
				// Note: Reddit enforces API call rate limits, which may cause API calls to fail unless throttling is implemented in method fetchRecords

				// In the Simple Data Pipe UI the user gets to choose from: 
				//  -> All data sets 
				//  -> "Hello Reddit, it's Sacha Baron Cohen, star of action ..."
				//  -> "IamA Transportation worker that specializes in road signs. AMA!"
				//  -> ...

				// sort list by display label and attach to the data pipe configuration; if present, the ALL_DATA option should be displayed first
				pipe.tables = dataSets.sort(function (dataSet1, dataSet2) {
																		if(! dataSet1.label)	{ // ALL_DATA (only property labelPlural is defined)
																			return -1;
																		}

																		if(! dataSet2.label) {// ALL_DATA (only property labelPlural is defined)
																			return 1;
																		}

																		return dataSet1.label.localeCompare(dataSet2.label);
																	   });
				// invoke callback and pass along the updated data pipe configuration, which now includes a list of reddit data sets the user
				// gets to choose from.
				return done(null, pipe);

		}); // request.get

	}; // getRedditDataSetList


	/*
	 * ---------------------------------------------------------------------------------------
	 * Override general connector methods:
	 *  - doConnectStep: verify that OAuth information is still valid
	 *  - fetchRecords:  load data from data source
	 * ---------------------------------------------------------------------------------------
	 */

	/**
	* Customization might be required.
	* During data pipe runs, this method is invoked first. Add custom code as required, for example to verify that the 
	* OAuth token has not expired.
	* @param done: callback that must be called when the connection is established
	* @param pipeRunStep
	* @param pipeRunStats
	* @param pipeRunLog
	* @param pipe
	* @param pipeRunner
	*/
	this.doConnectStep = function( done, pipeRunStep, pipeRunStats, pipeRunLog, pipe, pipeRunner ){

		//
		// Obtain new access token before trying to fetch data. Access tokens expire after an hour. 
		// See https://github.com/reddit/reddit/wiki/OAuth2
		//
		request.post(
				    {
				    	uri: 'https://ssl.reddit.com/api/v1/access_token', 
				    	headers: {
				            		'User-Agent' : userAgentHTTPHeaderValue,
				            		'Authorization' : 'Basic ' + new Buffer(pipe.clientId + ':' + pipe.clientSecret).toString('base64')
				            	 },
				    	form: {
				    			grant_type : 'refresh_token',
				            	refresh_token : pipe.oAuth.refreshToken
				    	      },
			        },
			        function(err, response, body) {

			    		if(err) {
			    			// there was a problem with the request; abort processing
			    			// by calling the callback and passing along an error message
			    			pipeRunLog.error('OAuth token refresh for data pipe ' +  pipe._id + ' failed due to error: ' + err);
							return done('OAuth token refresh error: ' + err); 
			    		}

			    		// Sample body:
			    		//              {
			    		//           	 "access_token": "5368999-SryekB08157Pp7PZ-lfn654J1E", 
			    		//               "token_type": "bearer", 
			    		//               "expires_in": 3600, 
			    		//               "scope": "identity read"
			    	    //              }
			    					    		
			    		var accessToken = JSON.parse(body).access_token;
			    		if(accessToken) {
			    			pipeRunLog.info('OAuth access token for data pipe ' + pipe._id + ' was refreshed.');
			    			pipe.oAuth.accessToken = accessToken;
			    			return done();	
			    		}
			    		else {
			    			pipeRunLog.error('OAuth access token for data pipe ' + pipe._id + ' could not be retrieved from reddit response: ' + util.inspect(body,3));			    			
			    			return done('OAuth access token could not be refreshed.');	
			    		}
					});

	}; // doConnectStep

	/*
	 * Customization is mandatory!
	 * Implement the code logic to fetch data from the source, optionally enrich it and store it in Cloudant.
	 * @param dataSet - dataSet.name contains the data set name that was (directly or indirectly) selected by the user
	 * @param done(err) - callback funtion to be invoked after processing is complete (or a fatal error has been encountered)
	 * @param pipe - data pipe configuration
	 * @param pipeRunLog - a dedicated logger instance that is only available during data pipe runs
	 */
	this.fetchRecords = function( dataSet, pushRecordFn, done, pipeRunStep, pipeRunStats, pipeRunLog, pipe, pipeRunner ){

		// The data set is typically selected by the user in the "Filter Data" panel during the pipe configuration step
		// dataSet: {name: 'data set name'}. However, if you enabled the ALL option (see get Tables) and it was selected, 
		// the fetchRecords function is invoked asynchronously once for each data set.
		// Note: Reddit enforces API call rules: https://github.com/reddit/reddit/wiki/API. 

		// Bunyan logging - https://github.com/trentm/node-bunyan
		// The log file is attached to the pipe run document, which is stored in the Cloudant repository database named pipe_db.
		// To enable debug logging, set environment variable DEBUG to '*' or to 'sdp-pipe-run' (without the quotes).
		pipeRunLog.info('Fetching comments for data set ' + dataSet.name + ' from reddit.');

		var requestOptions = {
			            url : 'https://oauth.reddit.com/r/iAMA/comments/'+ dataSet.name, // GET [/r/subreddit]/comments/article
			            headers: {
			            			'User-Agent' : userAgentHTTPHeaderValue,
			            			'Authorization' : 'bearer ' + pipe.oAuth.accessToken
			            }
			        };

		// e.g. https://oauth.reddit.com/r/iAMA/hot?count=10 (https://www.reddit.com/r/iAMA/hot.json?count=10) 
		request.get(requestOptions, function(err, response, body) {

	    		if(err) {
	    			// there was a problem with the request; abort processing
					pipeRunLog.error('AMA fetch request error: ' + err);
					return done(err, pipe); 
	    		}

	    		if(response.statusCode >= 300) {
					pipeRunLog.error('AMA Fetch request returned status code ' + response.statusCode);
					pipeRunLog.error(util.inspect(response,5));
					// abort processing
					return done('AMA Fetch request returned status code ' + response.statusCode, null); 
	    		}

	    		// process the returned data 
	    		// ...

	    		// save relevant information in Cloudant
	    		pushRecordFn(JSON.parse(body));

				// Invoke done callback to indicate that data set dataSet has been processed. 
				// Parameters:
				//  done()                                      // no parameter; processing completed successfully. no status message text is displayed to the end user in the monitoring view
				//  done({infoStatus: 'informational message'}) // processing completed successfully. the value of the property infoStatus is displayed to the end user in the monitoring view
				//  done({errorStatus: 'error message'})        // a fatal error was encountered during processing. the value of the property infoStatus is displayed to the end user in the monitoring view
				//  done('error message')                       // deprecated; a fatal error was encountered during processing. the message is displayed to the end user in the monitoring view
				return done();

	    }); // request.get

	}; // fetchRecords

	/*
	 * Customization is not needed.
	 */
	this.getTablePrefix = function(){
		// The prefix is used to generate names for the Cloudant staging databases that store your data. 
		// The recommended value is the connector ID to assure uniqueness.
		return connectorInfo.id;
	};


} // function oAuthRedditConnector

//Extend event Emitter
util.inherits(oAuthRedditConnector, connectorExt);

module.exports = new oAuthRedditConnector();