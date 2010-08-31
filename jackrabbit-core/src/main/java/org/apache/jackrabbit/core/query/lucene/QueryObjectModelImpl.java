/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.core.query.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.query.InvalidQueryException;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.qom.Column;
import javax.jcr.query.qom.Ordering;
import javax.jcr.query.qom.PropertyValue;
import javax.jcr.query.qom.Selector;

import org.apache.jackrabbit.commons.iterator.RowIteratorAdapter;
import org.apache.jackrabbit.core.ItemManager;
import org.apache.jackrabbit.core.SessionImpl;
import org.apache.jackrabbit.core.query.PropertyTypeRegistry;
import org.apache.jackrabbit.core.query.lucene.join.QueryEngine;
import org.apache.jackrabbit.core.query.lucene.join.SelectorRow;
import org.apache.jackrabbit.core.query.lucene.join.SimpleQueryResult;
import org.apache.jackrabbit.spi.commons.query.qom.BindVariableValueImpl;
import org.apache.jackrabbit.spi.commons.query.qom.DefaultTraversingQOMTreeVisitor;
import org.apache.jackrabbit.spi.commons.query.qom.QueryObjectModelTree;
import org.apache.jackrabbit.spi.commons.query.qom.SelectorImpl;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/**
 * <code>QueryObjectModelImpl</code>...
 */
public class QueryObjectModelImpl extends AbstractQueryImpl {

    /**
     * The query object model tree.
     */
    private final QueryObjectModelTree qomTree;

    /**
     * Creates a new query instance from a query string.
     *
     * @param session the session of the user executing this query.
     * @param itemMgr the item manager of the session executing this query.
     * @param index   the search index.
     * @param propReg the property type registry.
     * @param qomTree the query object model tree.
     * @throws InvalidQueryException if the QOM tree is invalid.
     */
    public QueryObjectModelImpl(SessionImpl session,
                                ItemManager itemMgr,
                                SearchIndex index,
                                PropertyTypeRegistry propReg,
                                QueryObjectModelTree qomTree)
            throws InvalidQueryException {
        super(session, itemMgr, index, propReg);
        this.qomTree = qomTree;
        checkNodeTypes();
        extractBindVariableNames();
    }

    /**
     * Returns <code>true</code> if this query node needs items under
     * /jcr:system to be queried.
     *
     * @return <code>true</code> if this query node needs content under
     *         /jcr:system to be queried; <code>false</code> otherwise.
     */
    public boolean needsSystemTree() {
        // TODO: analyze QOM tree
        return true;
    }

    //-------------------------< ExecutableQuery >------------------------------

    /**
     * Executes this query and returns a <code>{@link QueryResult}</code>.
     *
     * @param offset the offset in the total result set
     * @param limit  the maximum result size
     * @return a <code>QueryResult</code>
     * @throws RepositoryException if an error occurs
     */
    public QueryResult execute(long offset, long limit)
            throws RepositoryException {
        final LuceneQueryFactoryImpl factory = new LuceneQueryFactoryImpl(
                session, index.getContext().getHierarchyManager(),
                index.getNamespaceMappings(), index.getTextAnalyzer(),
                index.getSynonymProvider(), index.getIndexFormatVersion(),
                getBindVariableValues());

        QueryEngine engine = new QueryEngine(session) {
            @Override
            protected QueryResult execute(
                    Column[] columns, Selector selector,
                    javax.jcr.query.qom.Constraint constraint,
                    Ordering[] orderings)
                    throws RepositoryException {
                Map<String, NodeType> selectorMap = getSelectorNames(selector);
                final String[] selectorNames =
                    selectorMap.keySet().toArray(new String[selectorMap.size()]);

                final Map<String, PropertyValue> columnMap =
                    getColumnMap(columns, selectorMap);
                final String[] columnNames =
                    columnMap.keySet().toArray(new String[columnMap.size()]);

                List<Row> rows = new ArrayList<Row>();

                String selectorName = selector.getSelectorName();
                Query query = factory.create(selector);
                if (constraint != null) {
                    BooleanQuery b = new BooleanQuery();
                    b.add(query, Occur.MUST);
                    b.add(factory.create(constraint), Occur.MUST);
                    query = b;
                }
                try {
                    JackrabbitIndexSearcher searcher = new JackrabbitIndexSearcher(
                            session, index.getIndexReader(),
                            index.getContext().getItemStateManager());
                    QueryHits hits = searcher.evaluate(query);
                    ScoreNode node = hits.nextScoreNode();
                    while (node != null) {
                        rows.add(new SelectorRow(
                                columnMap, evaluator, selectorName,
                                session.getNodeByIdentifier(node.getNodeId().toString()),
                                node.getScore()));
                        hits.nextScoreNode();
                    }
                } catch (IOException e) {
                    throw new RepositoryException(e);
                }

                QueryResult result = new SimpleQueryResult(
                        columnNames, selectorNames, new RowIteratorAdapter(rows));
                return sort(result, orderings);
            }

        };

        // FIXME: handle offset and limit
        return engine.execute(
                qomTree.getColumns(), qomTree.getSource(),
                qomTree.getConstraint(), qomTree.getOrderings());
    }

    //--------------------------< internal >------------------------------------

    /**
     * Extracts all {@link BindVariableValueImpl} from the {@link #qomTree}
     * and adds it to the set of known variable names.
     */
    private void extractBindVariableNames() {
        try {
            qomTree.accept(new DefaultTraversingQOMTreeVisitor() {
                public Object visit(BindVariableValueImpl node, Object data) {
                    addVariableName(node.getBindVariableQName());
                    return data;
                }
            }, null);
        } catch (Exception e) {
            // will never happen
        }
    }

    /**
     * Checks if the selector node types are valid.
     *
     * @throws InvalidQueryException if one of the selector node types is
     *                               unknown.
     */
    private void checkNodeTypes() throws InvalidQueryException {
        try {
            qomTree.accept(new DefaultTraversingQOMTreeVisitor() {
                public Object visit(SelectorImpl node, Object data) throws Exception {
                    String ntName = node.getNodeTypeName();
                    if (!session.getNodeTypeManager().hasNodeType(ntName)) {
                        throw new Exception(ntName + " is not a known node type");
                    }
                    return super.visit(node, data);
                }
            }, null);
        } catch (Exception e) {
            throw new InvalidQueryException(e.getMessage());
        }
    }
}
