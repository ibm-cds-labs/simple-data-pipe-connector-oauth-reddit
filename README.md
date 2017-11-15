# Simple Data Pipe connector boilerplate for reddit

:no_entry_sign: This project is no longer maintained.

This [Simple Data Pipe](https://developer.ibm.com/clouddataservices/simple-data-pipe/) connector for reddit boilerplate has been customized for [reddit.com](http://www.reddit.com) OAuth access. You can build your own special purpose connector by implementing the `getRedditDataSetList` and `fetchRecords` functions in `lib/index.js` to fetch the desired data from reddit and optionally enrich it.

The data property for the selected article and every comment in the comment tree is retrieved and stored in Cloudant (See [https://www.reddit.com/dev/api](https://www.reddit.com/dev/api) for more information).
Since every comment in the comment tree is retrieved and stored individually the replies property for each document is not stored in Cloudant.
Two additional properties are added to each document:
 
1. tree_level: The level at which the article or comment appears in the tree.
2. tree_path: The path from the comment up to the article document (an array of ids starting with the comment's parent id and moving all the way up to the article id - the root of the tree).

##### Sample Record structure
```json
{
 "..." : "<cloudant document properties such as _id and _rev>",
 "subreddit_id": "t5_xxxxx",
 "banned_by": null,
 "removal_reason": null,
 "link_id": "t3_xxxxxx",
 "likes": null,
 "user_reports": [],
 "saved": false,
 "id": "xxxxxxx",
 "gilded": 0,
 "archived": false,
 "report_reasons": null,
 "author": "<author>",
 "parent_id": "t3_xxxxxx",
 "score": 1,
 "approved_by": null,
 "controversiality": 0,
 "body": "Do you guys need any software engineers? ",
 "edited": false,
 "author_flair_css_class": null,
 "downs": 0,
 "body_html": "&lt;div class=\"md\"&gt;&lt;p&gt;Do you guys need any software engineers? &lt;/p&gt;\n&lt;/div&gt;",
 "stickied": false,
 "subreddit": "IAmA",
 "score_hidden": false,
 "name": "t1_xxxxxxx",
 "created": 1458803108,
 "author_flair_text": null,
 "created_utc": 1458774308,
 "ups": 1,
 "mod_reports": [],
 "num_reports": null,
 "distinguished": null,
 "tree_path": [
  "t1_xxxxxx",
  "t1_xxxxxx",
  "t3_xxxxxx"
 ],
 "tree_level": 3,
 "pt_type": "<subreddit_id>"		 		 
}
```

### Pre-requisites

##### General 
 A valid reddit user id is required to use this connector.

##### Deploy the Simple Data Pipe

 [Deploy the Simple Data Pipe in Bluemix](https://github.com/ibm-cds-labs/simple-data-pipe) using the Deploy to Bluemix button or manually.

##### Services

This connector does not require any additional Bluemix service.

##### Install the Simple Data Pipe connector boilerplate for reddit

  When you [follow these steps to install this connector](https://github.com/ibm-cds-labs/simple-data-pipe/wiki/Installing-a-Simple-Data-Pipe-Connector), add the following line to the dependencies list in the `package.json` file: 

```
"simple-data-pipe-connector-oauth-reddit": "git://github.com/ibm-cds-labs/simple-data-pipe-connector-oauth-reddit.git",
```

##### Enable OAuth support and collect connectivity information

 You need to register the Simple Data Pipe application before you can use it to load data.
 1. Open the [reddit](http://www.reddit.com) web page and log in.
 2. Click **Preferences** and select the **apps** tab.
 3. **Create another app...**
 4. Assign an application **name** and enter an optional **description**.
 5. As _redirect URL_ enter `https://<simple-data-...mybluemix.net>/authCallback`.
   > Replace `<simple-data-...mybluemix.net>` with the fully qualified host name of your Simple Data Pipe application on Bluemix.

 6. Click **create app**.
 7. Copy the application id displayed under your application name (e.g. vv5ulJR3...20Q) and the secret (e.g. j60....CFSyAmSY).


### Using the Simple Data Pipe OAuth sample connector 

To configure and run a pipe

1. Open the Simple Data Pipe web console.
2. Select __Create A New Pipe__.
3. Select __Reddit OAuth Data Source__ for the __Type__ when creating a new pipe  
4. In the _Connect_ page, enter the _application id_ and _secret_ from the reddit app preferences page. 
5. Select the data set (or data sets) to be loaded.
6. Schedule or run the data pipe now.

#### License 

Copyright [2016] IBM Cloud Data Services

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
