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

import static javax.jcr.PropertyType.DATE;
import static javax.jcr.PropertyType.DOUBLE;
import static javax.jcr.PropertyType.LONG;
import static javax.jcr.PropertyType.NAME;
import static javax.jcr.PropertyType.STRING;
import static javax.jcr.PropertyType.UNDEFINED;
import static javax.jcr.query.qom.QueryObjectModelConstants.JCR_OPERATOR_EQUAL_TO;
import static javax.jcr.query.qom.QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN;
import static javax.jcr.query.qom.QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN_OR_EQUAL_TO;
import static javax.jcr.query.qom.QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN;
import static javax.jcr.query.qom.QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN_OR_EQUAL_TO;
import static javax.jcr.query.qom.QueryObjectModelConstants.JCR_OPERATOR_LIKE;
import static javax.jcr.query.qom.QueryObjectModelConstants.JCR_OPERATOR_NOT_EQUAL_TO;
import static org.apache.jackrabbit.core.query.lucene.FieldNames.PROPERTIES;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.query.InvalidQueryException;
import javax.jcr.query.qom.And;
import javax.jcr.query.qom.BindVariableValue;
import javax.jcr.query.qom.ChildNode;
import javax.jcr.query.qom.Comparison;
import javax.jcr.query.qom.Constraint;
import javax.jcr.query.qom.DescendantNode;
import javax.jcr.query.qom.DynamicOperand;
import javax.jcr.query.qom.FullTextSearch;
import javax.jcr.query.qom.Literal;
import javax.jcr.query.qom.Not;
import javax.jcr.query.qom.Or;
import javax.jcr.query.qom.PropertyExistence;
import javax.jcr.query.qom.PropertyValue;
import javax.jcr.query.qom.SameNode;
import javax.jcr.query.qom.Selector;
import javax.jcr.query.qom.StaticOperand;

import org.apache.jackrabbit.core.HierarchyManager;
import org.apache.jackrabbit.core.SessionImpl;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.commons.conversion.NamePathResolver;
import org.apache.jackrabbit.spi.commons.name.NameConstants;
import org.apache.jackrabbit.spi.commons.query.qom.BindVariableValueImpl;
import org.apache.jackrabbit.spi.commons.query.qom.DefaultQOMTreeVisitor;
import org.apache.jackrabbit.spi.commons.query.qom.FullTextSearchImpl;
import org.apache.jackrabbit.spi.commons.query.qom.JoinConditionImpl;
import org.apache.jackrabbit.spi.commons.query.qom.JoinImpl;
import org.apache.jackrabbit.spi.commons.query.qom.PropertyExistenceImpl;
import org.apache.jackrabbit.spi.commons.query.qom.SelectorImpl;
import org.apache.jackrabbit.spi.commons.query.qom.SourceImpl;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/**
 * <code>LuceneQueryFactoryImpl</code> implements a lucene query factory.
 */
public class LuceneQueryFactoryImpl implements LuceneQueryFactory {

    /**
     * Session of the user executing this query
     */
    private final SessionImpl session;

    /**
     * The hierarchy manager.
     */
    private final HierarchyManager hmgr;

    /**
     * Namespace mappings to internal prefixes
     */
    private final NamespaceMappings nsMappings;

    /**
     * NamePathResolver to map namespace mappings to internal prefixes
     */
    private final NamePathResolver npResolver;

    /**
     * The analyzer instance to use for contains function query parsing
     */
    private final Analyzer analyzer;

    /**
     * The synonym provider or <code>null</code> if none is configured.
     */
    private final SynonymProvider synonymProvider;

    /**
     * The index format version.
     */
    private final IndexFormatVersion version;

    /**
     * The Bind Variable values.
     */
    private final Map<Name, Value> bindVariables;

    /**
     * Creates a new lucene query factory.
     *
     * @param session         the session that executes the query.
     * @param scs             the sort comparator source of the index.
     * @param hmgr            the hierarchy manager of the workspace.
     * @param nsMappings      the index internal namespace mappings.
     * @param analyzer        the analyzer of the index.
     * @param synonymProvider the synonym provider of the index.
     * @param version         the version of the index format.
     * @param bindVariables   the bind variable values of the query
     */
    public LuceneQueryFactoryImpl(SessionImpl session,
                                  HierarchyManager hmgr,
                                  NamespaceMappings nsMappings,
                                  Analyzer analyzer,
                                  SynonymProvider synonymProvider,
                                  IndexFormatVersion version,
                                  Map<Name, Value> bindVariables) {
        this.session = session;
        this.hmgr = hmgr;
        this.nsMappings = nsMappings;
        this.analyzer = analyzer;
        this.synonymProvider = synonymProvider;
        this.version = version;
        this.npResolver = NamePathResolverImpl.create(nsMappings);
        this.bindVariables = bindVariables;
    }

    /**
     * {@inheritDoc}
     */
    public Query create(Selector selector) throws RepositoryException {
        List<Term> terms = new ArrayList<Term>();
        String mixinTypesField = npResolver.getJCRName(NameConstants.JCR_MIXINTYPES);
        String primaryTypeField = npResolver.getJCRName(NameConstants.JCR_PRIMARYTYPE);

        NodeTypeManager ntMgr = session.getWorkspace().getNodeTypeManager();
        NodeType base = null;
        try {
            base = ntMgr.getNodeType(selector.getNodeTypeName());
        } catch (RepositoryException e) {
            // node type does not exist
        }

        Name name = session.getQName(selector.getNodeTypeName());
        if (base != null && base.isMixin()) {
            // search for nodes where jcr:mixinTypes is set to this mixin
            Term t = new Term(FieldNames.PROPERTIES,
                    FieldNames.createNamedValue(
                            mixinTypesField, npResolver.getJCRName(name)));
            terms.add(t);
        } else {
            // search for nodes where jcr:primaryType is set to this type
            Term t = new Term(FieldNames.PROPERTIES,
                    FieldNames.createNamedValue(
                            primaryTypeField, npResolver.getJCRName(name)));
            terms.add(t);
        }

        // now search for all node types that are derived from base
        if (base != null) {
            NodeTypeIterator allTypes = ntMgr.getAllNodeTypes();
            while (allTypes.hasNext()) {
                NodeType nt = allTypes.nextNodeType();
                NodeType[] superTypes = nt.getSupertypes();
                if (Arrays.asList(superTypes).contains(base)) {
                    Name n = session.getQName(nt.getName());
                    String ntName = nsMappings.translateName(n);
                    Term t;
                    if (nt.isMixin()) {
                        // search on jcr:mixinTypes
                        t = new Term(FieldNames.PROPERTIES,
                                FieldNames.createNamedValue(mixinTypesField, ntName));
                    } else {
                        // search on jcr:primaryType
                        t = new Term(FieldNames.PROPERTIES,
                                FieldNames.createNamedValue(primaryTypeField, ntName));
                    }
                    terms.add(t);
                }
            }
        }
        Query q;
        if (terms.size() == 1) {
            q = new JackrabbitTermQuery(terms.get(0));
        } else {
            BooleanQuery b = new BooleanQuery();
            for (Term term : terms) {
                b.add(new JackrabbitTermQuery(term), BooleanClause.Occur.SHOULD);
            }
            q = b;
        }
        return q;
    }

    public Query create(
            Constraint constraint, Map<String, NodeType> selectorMap)
            throws RepositoryException {
        if (constraint instanceof And) {
            return getAndQuery((And) constraint, selectorMap);
        } else if (constraint instanceof Or) {
            return getOrQuery((Or) constraint, selectorMap);
        } else if (constraint instanceof Not) {
            return getNotQuery((Not) constraint, selectorMap);
        } else if (constraint instanceof PropertyExistence) {
            return getPropertyExistenceQuery((PropertyExistence) constraint);
        } else if (constraint instanceof Comparison) {
            return getComparisonQuery((Comparison) constraint, selectorMap);
        } else if (constraint instanceof FullTextSearch) {
            return null; // FIXME
        } else if (constraint instanceof SameNode) {
            return null; // FIXME
        } else if (constraint instanceof ChildNode) {
            return null; // FIXME
        } else if (constraint instanceof DescendantNode) {
            return null; // FIXME
        } else {
            throw new UnsupportedRepositoryOperationException(
                    "Unknown constraint type: " + constraint);
        }
    }

    private BooleanQuery getAndQuery(And and, Map<String, NodeType> selectorMap)
            throws RepositoryException {
        BooleanQuery query = new BooleanQuery();
        addBooleanConstraint(query, and.getConstraint1(), MUST, selectorMap);
        addBooleanConstraint(query, and.getConstraint2(), MUST, selectorMap);
        return query;
    }

    private BooleanQuery getOrQuery(Or or, Map<String, NodeType> selectorMap)
            throws RepositoryException {
        BooleanQuery query = new BooleanQuery();
        addBooleanConstraint(query, or.getConstraint1(), SHOULD, selectorMap);
        addBooleanConstraint(query, or.getConstraint2(), SHOULD, selectorMap);
        return query;
    }

    private void addBooleanConstraint(
            BooleanQuery query, Constraint constraint, Occur occur,
            Map<String, NodeType> selectorMap)
            throws RepositoryException {
        if (occur == MUST && constraint instanceof And) {
            And and = (And) constraint;
            addBooleanConstraint(query, and.getConstraint1(), occur, selectorMap);
            addBooleanConstraint(query, and.getConstraint2(), occur, selectorMap);
        } else if (occur == SHOULD && constraint instanceof Or) {
            Or or = (Or) constraint;
            addBooleanConstraint(query, or.getConstraint1(), occur, selectorMap);
            addBooleanConstraint(query, or.getConstraint2(), occur, selectorMap);
        } else {
            query.add(create(constraint, selectorMap), occur);
        }
    }

    private NotQuery getNotQuery(Not not, Map<String, NodeType> selectorMap)
            throws RepositoryException {
        return new NotQuery(create(not.getConstraint(), selectorMap));
    }

    private Query getPropertyExistenceQuery(PropertyExistence property)
            throws RepositoryException {
        String name = npResolver.getJCRName(session.getQName(
                property.getPropertyName()));
        return Util.createMatchAllQuery(name, version);
    }

    private Query getComparisonQuery(
            Comparison comparison, Map<String, NodeType> selectorMap)
            throws RepositoryException {
        DynamicOperand operand = comparison.getOperand1();
        if (operand instanceof PropertyValue) {
            PropertyValue property = (PropertyValue) operand;
            String field = npResolver.getJCRName(session.getQName(
                    property.getPropertyName()));
            int type = PropertyType.UNDEFINED;
            NodeType nt = selectorMap.get(property.getSelectorName());
            if (nt != null) {
                for (PropertyDefinition pd : nt.getPropertyDefinitions()) {
                    if (pd.getName().equals(property.getPropertyName())) {
                        type = pd.getRequiredType();
                    }
                }
            }
            return getPropertyValueQuery(
                    field, comparison.getOperator(),
                    getValue(comparison.getOperand2()), type);
        } else {
            throw new UnsupportedRepositoryOperationException(); // FIXME
        }
    }

    private Query getPropertyValueQuery(
            String field, String operator, Value value, int type)
            throws RepositoryException {
        Term term = getTerm(field, getValueString(value, type));
        if (JCR_OPERATOR_EQUAL_TO.equals(operator)) {
            return new JackrabbitTermQuery(term);
        } else if (JCR_OPERATOR_GREATER_THAN.equals(operator)) {
            return new RangeQuery(term, getTerm(field, "\uFFFF"), false);
        } else if (JCR_OPERATOR_GREATER_THAN_OR_EQUAL_TO.equals(operator)) {
            return new RangeQuery(term, getTerm(field, "\uFFFF"), true);
        } else if (JCR_OPERATOR_LESS_THAN.equals(operator)) {
            return new RangeQuery(getTerm(field, ""), term, false);
        } else if (JCR_OPERATOR_LESS_THAN_OR_EQUAL_TO.equals(operator)) {
            return new RangeQuery(getTerm(field, ""), term, true);
        } else if (JCR_OPERATOR_NOT_EQUAL_TO.equals(operator)) {
            BooleanQuery or = new BooleanQuery();
            or.add(new RangeQuery(getTerm(field, ""), term, false), SHOULD);
            or.add(new RangeQuery(term, getTerm(field, "\uFFFF"), false), SHOULD);
            return or;
        } else if (JCR_OPERATOR_LIKE.equals(operator)) {
            throw new UnsupportedRepositoryOperationException(); // FIXME
        } else {
            throw new UnsupportedRepositoryOperationException(); // FIXME
        }
    }

    private Term getTerm(String field, String value) {
        return new Term(PROPERTIES, FieldNames.createNamedValue(field, value));
    }

    private String getValueString(Value value, int type)
            throws RepositoryException {
        switch (value.getType()) {
        case DATE:
            return DateField.dateToString(value.getDate().getTime());
        case DOUBLE:
            return DoubleField.doubleToString(value.getDouble());
        case LONG:
            return LongField.longToString(value.getLong());
        case NAME:
            return npResolver.getJCRName(session.getQName(value.getString()));
        default:
            String string = value.getString();
            if (type != UNDEFINED && type != STRING) {
                return getValueString(
                        session.getValueFactory().createValue(string, type),
                        UNDEFINED);
            } else {
                return string;
            }
        }
    }

    private Value getValue(StaticOperand operand) throws RepositoryException {
        if (operand instanceof Literal) {
            Literal literal = (Literal) operand;
            return literal.getLiteralValue();
        } else if (operand instanceof BindVariableValue) {
            BindVariableValue bind = (BindVariableValue) operand;
            Value value = bindVariables.get(session.getQName(
                    bind.getBindVariableName()));
            if (value != null) {
                return value;
            } else {
                throw new RepositoryException(
                        "Unknown variable name: " + bind.getBindVariableName());
            }
        } else {
            throw new UnsupportedRepositoryOperationException(
                    "Unknown static operand type: " + operand);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Query create(FullTextSearchImpl fts) throws RepositoryException {
        String fieldname;
        if (fts.getPropertyName() == null) {
            // fulltext on node
            fieldname = FieldNames.FULLTEXT;
        } else {
            // final path element is a property name
            Name propName = fts.getPropertyQName();
            StringBuffer tmp = new StringBuffer();
            tmp.append(nsMappings.getPrefix(propName.getNamespaceURI()));
            tmp.append(":").append(FieldNames.FULLTEXT_PREFIX);
            tmp.append(propName.getLocalName());
            fieldname = tmp.toString();
        }
        QueryParser parser = new JackrabbitQueryParser(
                fieldname, analyzer, synonymProvider);
        try {
            StaticOperand expr = fts.getFullTextSearchExpression();
            if (expr instanceof Literal) {
                return parser.parse(
                        ((Literal) expr).getLiteralValue().getString());
            } else if (expr instanceof BindVariableValueImpl) {
                Value value = this.bindVariables.get(
                        ((BindVariableValueImpl) expr).getBindVariableQName());
                if (value == null) {
                    throw new InvalidQueryException("Bind variable not bound");
                }
                return parser.parse(value.getString());
            } else {
                throw new RepositoryException(
                        "Unknown static operand type: " + expr);
            }
        } catch (ParseException e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Query create(PropertyExistenceImpl prop) throws RepositoryException {
        String propName = npResolver.getJCRName(prop.getPropertyQName());
        return Util.createMatchAllQuery(propName, version);
    }

    /**
     * {@inheritDoc}
     */
    public MultiColumnQuery create(SourceImpl source) throws RepositoryException {
        // source is either selector or join
        try {
            return (MultiColumnQuery) source.accept(new DefaultQOMTreeVisitor() {
                public Object visit(JoinImpl node, Object data) throws Exception {
                    return create(node);
                }

                public Object visit(SelectorImpl node, Object data) throws Exception {
                    return MultiColumnQueryAdapter.adapt(
                            create((Selector) node), node.getSelectorQName());
                }
            }, null);
        } catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public MultiColumnQuery create(JoinImpl join) throws RepositoryException {
        MultiColumnQuery left = create((SourceImpl) join.getLeft());
        MultiColumnQuery right = create((SourceImpl) join.getRight());
        return new JoinQuery(left, right, join.getJoinTypeInstance(),
                (JoinConditionImpl) join.getJoinCondition(), nsMappings, hmgr);
    }
}
