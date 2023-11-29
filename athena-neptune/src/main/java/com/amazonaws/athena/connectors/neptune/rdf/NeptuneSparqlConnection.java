/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.neptune.rdf;

import com.amazonaws.athena.connectors.neptune.NeptuneConnection;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

//import org.eclipse.rdf4j.model.IRI;

@SuppressWarnings("deprecation")
public class NeptuneSparqlConnection extends NeptuneConnection 
{
    private static final Logger logger = LoggerFactory.getLogger(NeptuneSparqlConnection.class);

    String connectionString = null;
    boolean trimURI = false;
    NeptuneSparqlRepository neptuneSparqlRepo = null;
    RepositoryConnection connection = null;
    TupleQueryResult queryResult = null;

    public NeptuneSparqlConnection(String neptuneEndpoint, String neptunePort, boolean enabledIAM, String region) 
    {
        super(neptuneEndpoint, neptunePort, enabledIAM, region);
        this.connectionString = "https://" + neptuneEndpoint + ":" + neptunePort;
        try {
            connect();
        }
        catch (NeptuneSigV4SignerException e) {
            logger.error("SIGV4 exception", e);
            throw new RuntimeException(e);
        }
    }

    private void connect() throws NeptuneSigV4SignerException
    {
        safeCloseConn();
        if (isEnabledIAM()) {
            logger.info("Connecting with IAM auth to " + this.connectionString);
            final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
            this.neptuneSparqlRepo = new NeptuneSparqlRepository(this.connectionString, awsCredentialsProvider, getRegion());
        } 
        else {
            logger.info("Connecting without IAM auth to " + this.connectionString);
            this.neptuneSparqlRepo = new NeptuneSparqlRepository(this.connectionString);
        }
        logger.info("Initializing");
        this.neptuneSparqlRepo.initialize();
        logger.info("Getting connection");
        this.connection = this.neptuneSparqlRepo.getConnection();
        logger.info("Connection obtained");
    }

    public boolean hasNext() 
    {
        if (this.queryResult == null) {
            return false;
        }
        if (this.queryResult.hasNext()) {
            return true;
        }
        safeCloseResult();
        return false;
    }

    public Map<String, Object> next(boolean trimURI) 
    {
        Map<String, Object> ret = new HashMap<String, Object>();
        BindingSet bindingSet = this.queryResult.next();

        for (String varName : bindingSet.getBindingNames()) {
            Value val = bindingSet.getValue(varName);
            String sval = val.stringValue();
            Object oval = sval;
            if (this.trimURI && val instanceof URI) {
                oval = ((URI) val).getLocalName();
                logger.debug("URI " + varName + "=" + oval);
            } 
            else if (this.trimURI && val instanceof IRI) {
                oval = ((IRI) val).getLocalName();
                logger.debug("IRI " + varName + "=" + oval);
            } 
            else if (val instanceof Literal) {
                Literal lit = (Literal) val;
                IRI type = lit.getDatatype();
                logger.debug("Type of " + varName + " is " + type);
                if (type.equals(XMLSchema.BOOLEAN)) {
                    oval = lit.booleanValue();
                    logger.debug("Boolean " + varName + "=" + oval);
                } 
                else if (type.equals(XMLSchema.DATE)) {
                    oval = lit.calendarValue();
                    logger.debug("Date " + varName + "=" + oval);
                } 
                else if (type.equals(XMLSchema.DATETIME)) {
                    oval = lit.calendarValue();
                    logger.debug("Datetime " + varName + "=" + oval);
                } 
                else if (type.equals(XMLSchema.DECIMAL)) {
                    oval = lit.decimalValue();
                    logger.debug("Decimal " + varName + "=" + oval);
                } 
                else if (type.equals(XMLSchema.DOUBLE)) {
                    oval = lit.doubleValue();
                    logger.debug("Double " + varName + "=" + oval);
                } 
                else if (type.equals(XMLSchema.FLOAT)) {
                    oval = lit.floatValue();
                    logger.debug("Float " + varName + "=" + oval);
                } 
                else if (type.equals(XMLSchema.INT)) {
                    oval = lit.intValue();
                    logger.debug("Int" + varName + "=" + oval);
                } 
                else if (type.equals(XMLSchema.INTEGER)) {
                    oval = lit.integerValue();
                    logger.debug("Integer " + varName + "=" + oval);
                } 
                else if (type.equals(XMLSchema.LONG)) {
                    oval = lit.longValue();
                    logger.debug("Long " + varName + "=" + oval);
                }
            }
            ret.put(varName, oval);
        }

        logger.debug("Record map " + ret);
        return ret;
    }

    public void safeCloseRepo() 
    {
        logger.info("Closing repo");
        safeCloseResult();
        safeCloseConn();
        if (this.neptuneSparqlRepo != null) {
            try {
                this.neptuneSparqlRepo.shutDown();
                this.neptuneSparqlRepo = null;
            } 
            catch (RepositoryException e) {
                e.printStackTrace();
                logger.error("Error closing repo", e);
            }
        }
    }

    private void safeCloseConn() 
    {
        logger.info("Closing connection");
        if (this.connection != null) {
            try {
                this.connection.close();
                this.connection = null;
            } 
            catch (RepositoryException e) {
                e.printStackTrace();
                logger.error("Error closing connection", e);
            }
        }
    }

    private void safeCloseResult() 
    {
        logger.info("Closing result");
        if (this.queryResult != null) {
            try {
                this.queryResult.close();
                this.queryResult = null;
            } 
            catch (RepositoryException e) {
                e.printStackTrace();
                logger.error("Error closing query result", e);
            }
        }
    }

    public void runQuery(String sparql) 
    {
        logger.info("Running SPARQL query " + sparql);
        TupleQuery tupleQuery = this.connection.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
        this.queryResult = tupleQuery.evaluate();
    }
}
