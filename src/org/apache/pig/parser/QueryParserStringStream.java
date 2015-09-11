/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.parser;

import java.io.IOException;
import java.util.HashMap;

import org.antlr.runtime.ANTLRStringStream;
import org.apache.pig.parser.metadataparser.MetaDataParser;
import org.apache.pig.parser.metadataparser.Table;

/**
 * Pig's Implementation class for String stream, used to make ANTLR case insensitive
 *  while preserving case.
 */
public class QueryParserStringStream extends ANTLRStringStream {
    public QueryParserStringStream(String input, String source) throws IOException {
    	super(changeQueryString(input));
        this.name = source;
    }

    @Override
    public int LA(int i) {
        return QueryParserStreamUtil.LA( i, n, p, data );
    }

    /**
     * Based on the metadata information to see whether there is need to change user's input query string
     * <p>
     * @param input the query users have submitted
     * @return the new query string
     */
    private static String changeQueryString(String input){
        try {
        	// first check whether there is predicates in the query plan, if not, we are done
        	System.out.println(input);
        	String[] splitByFilter = input.split("filter",2);
			if(splitByFilter.length > 1){
	        	String beforeFilter = splitByFilter[0];
	        	String afterFilter = splitByFilter[1];
	        	// split the string by "by", to get the table name and the colName
				String[] splitByBy = afterFilter.split("by",2);
				if(splitByBy.length > 1){
					String tableName = splitByBy[0].trim();
					String afterBy = splitByBy[1].trim();
					String[] splitByPredicate = splitByBy[1].split("[><(==)(!=)]");
					String colName = splitByPredicate[0].trim();

			    	MetaDataParser parser = new MetaDataParser();
			    	HashMap<String, Table> map = parser.getTableDict();
			    	//check whether this table is in the metadata.
			    	if(map.containsKey(tableName)){
			    		Table table = map.get(tableName);
			    		String afterColName;
			    		// check whether the column is indexed by position (using $ notation), if it is, replace with the column name.
			    		if(colName.charAt(0)== '$'){
			    			afterColName = afterBy.split("\\" + colName, 2)[1];
			    			String[] split = colName.split("\\$",2);
			    			int position = Integer.parseInt(split[1]);
			    			colName = table.getAllCol().get(position).getName();

			    		} else {
			    			afterColName = afterBy.split(colName,2)[1];
			    		}
			    		HashMap<String,String> usefulDependency = table.getUsefulDependency();
			    		String replace = usefulDependency.get(colName);
			    		if(replace != null){
                            // rebuild the query and change the input as new query.
			    			input = beforeFilter + "filter "+ tableName + " by " + replace + afterColName;


			    		}

			    	}

				}

			}
		} catch (Exception e) {
			// if any of the previous code cause exception, don't change the input query.
		}

        return input;


    }
}

