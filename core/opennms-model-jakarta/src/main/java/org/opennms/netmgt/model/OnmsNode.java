/*
 * Licensed to The OpenNMS Group, Inc (TOG) under one or more
 * contributor license agreements.  See the LICENSE.md file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * TOG licenses this file to You under the GNU Affero General
 * Public License Version 3 (the "License") or (at your option)
 * any later version.  You may not use this file except in
 * compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *      https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
package org.opennms.netmgt.model;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import jakarta.persistence.AttributeOverride;
import jakarta.persistence.AttributeOverrides;
import jakarta.persistence.CascadeType;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import jakarta.persistence.SecondaryTable;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import jakarta.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonValue;

import org.hibernate.annotations.Filter;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.netmgt.events.api.EventConstants;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.model.events.AddEventVisitor;
import org.opennms.netmgt.model.events.DeleteEventVisitor;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.model.events.NodeLabelChangedEventBuilder;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.opennms.netmgt.model.jakarta.converter.NodeLabelSourceConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;


/**
 * Contains information on nodes discovered and potentially managed by OpenNMS.
 * sys* properties map to SNMP MIB 2 system table information.
 *
 * @hibernate.class table="node"
 */
@Entity()
@Table(name="node")
@SecondaryTable(name="pathOutage")
@Filter(name=FilterManager.AUTH_FILTER_NAME, condition="exists (select distinct x.nodeid from node x join category_node cn on x.nodeid = cn.nodeid join category_group cg on cn.categoryId = cg.categoryId where x.nodeid = nodeid and cg.groupId in (:userGroups))")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class OnmsNode extends OnmsEntity implements Serializable, Comparable<OnmsNode> {
    private static final long serialVersionUID = 5326410037533354861L;
    private static final Logger LOG = LoggerFactory.getLogger(OnmsNode.class);

    /** identifier field */
    private Integer m_id;

    /** persistent field */
    private Date m_createTime = new Date();

    /** nullable persistent field */
    private OnmsNode m_parent;

    /** nullable persistent field */
    private NodeType m_type;

    /** nullable persistent field */
    private String m_sysObjectId;

    /** nullable persistent field */
    private String m_sysName;

    /** nullable persistent field */
    private String m_sysDescription;

    /** nullable persistent field */
    private String m_sysLocation;

    /** nullable persistent field */
    private String m_sysContact;

    /** nullable persistent field */
    private String m_label;

    /** timestamps for a flow exporting node */
    private Date m_lastIngressFlow;
    private Date m_lastEgressFlow;

    @Transient
    @JsonIgnore
    private String m_oldLabel;

    /** nullable persistent field */
    private NodeLabelSource m_labelSource;

    @Transient
    @JsonIgnore
    private NodeLabelSource m_oldLabelSource;

    /** nullable persistent field */
    private String m_netBiosName;

    /** nullable persistent field */
    private String m_netBiosDomain;

    /** nullable persistent field */
    private String m_operatingSystem;

    /** nullable persistent field */
    private Date m_lastCapsdPoll;

    private String m_foreignSource;

    private String m_foreignId;

    /** persistent field */
    private OnmsMonitoringLocation m_location;

    /** persistent field */
    private OnmsAssetRecord m_assetRecord;

    /** persistent field */
    private Set<OnmsIpInterface> m_ipInterfaces = new LinkedHashSet<>();

    /** persistent field */
    private Set<OnmsSnmpInterface> m_snmpInterfaces = new LinkedHashSet<>();

    private Set<OnmsCategory> m_categories = new LinkedHashSet<>();

    private Set<String> m_requisitionedCategories = new LinkedHashSet<>();

    private PathElement m_pathElement;

    private List<OnmsMetaData> m_metaData = new ArrayList<>();

    private List<OnmsMetaData> m_requisitionedMetaData = new ArrayList<>();
    private Integer nodeParentId;

    /**
     * <p>
     * Constructor for OnmsNode. This constructor should only be used
     * by JPA and by unit tests that do not need to persist the {@link OnmsNode}
     * in the database. It does not associate the {@link OnmsNode} with a
     * required {@link OnmsMonitoringLocation}.
     * </p>
     */
    public OnmsNode() {
        this(null);
    }

    /**
     * <p>Constructor for OnmsNode.</p>
     *
     * @param location The location where this node is located
     */
    public OnmsNode(final OnmsMonitoringLocation location) {
        // Set the location
        setLocation(location);
        m_assetRecord = new OnmsAssetRecord();
        m_assetRecord.setNode(this);
    }

    /**
     * <p>Constructor for OnmsNode.</p>
     *
     * @param location The location where this node is located
     * @param label The node label
     */
    public OnmsNode(final OnmsMonitoringLocation location, final String label) {
        this(location);
        // Set the label
        setLabel(label);
    }

    /**
     * Unique identifier for node.
     *
     * @return a {@link java.lang.Integer} object.
     */
    @Id
    @Column(name="nodeId", nullable=false)
    @SequenceGenerator(name="nodeSequence", sequenceName="nodeNxtId", allocationSize = 1)
    @GeneratedValue(generator="nodeSequence")
    @JsonIgnore
    public Integer getId() {
        return m_id;
    }

    /**
     * <p>getNodeId</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Transient
    public String getNodeId() {
        if (getId() != null) {
            return getId().toString();
        }
        return null;
    }

    /**
     * <p>setId</p>
     *
     * @param nodeid a {@link java.lang.Integer} object.
     */
    public void setId(Integer nodeid) {
        m_id = nodeid;
    }

    /**
     * <p>setNodeId</p>
     *
     * @param nodeid a {@link java.lang.String} object.
     */
    public void setNodeId(String nodeid) {
        setId(Integer.valueOf(nodeid));
    }

    /**
     * Time node was added to the database.
     *
     * @return a {@link java.util.Date} object.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="nodeCreateTime", nullable=false)
    public Date getCreateTime() {
        return m_createTime;
    }

    /**
     * <p>setCreateTime</p>
     *
     * @param nodecreatetime a {@link java.util.Date} object.
     */
    public void setCreateTime(Date nodecreatetime) {
        m_createTime = nodecreatetime;
    }

    /**
     * In the case that the node is virtual or an independent device in a chassis
     * that should be reflected as a subcomponent or "child", this field reflects
     * the nodeID of the chassis/physical node/"parent" device.
     *
     * @return a {@link org.opennms.netmgt.model.OnmsNode} object.
     */
    @JsonIgnore
    @ManyToOne(fetch=FetchType.LAZY)
    @JoinColumn(name="nodeParentID")
    public OnmsNode getParent() {
        return m_parent;
    }

    /**
     * <p>setParent</p>
     *
     * @param parent a {@link org.opennms.netmgt.model.OnmsNode} object.
     */
    public void setParent(OnmsNode parent) {
        m_parent = parent;
    }

    // This already maps to the existing column, readonly
    @Column(name = "nodeParentID",insertable = false, updatable = false)
    public Integer getNodeParentId() {
        return nodeParentId;
    }

    public void setNodeParentId(Integer nodeParentId) {
        this.nodeParentId = nodeParentId;
    }

    public enum NodeType {
        /**
         * The character returned if the node is active
         */
        ACTIVE('A'),

        /**
         * The character returned if the node is deleted
         */
        DELETED('D'),

        /**
         * The character returned if the node type is unset/unknown.
         */
        UNKNOWN(' ');

        private final char value;

        NodeType(char c) {
            value = c;
        }

        @JsonValue
        public char value() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public static NodeType getNodeTypeFromChar(char c) {
            for (NodeType nodeType: NodeType.values()) {
                if (nodeType.value == c)
                    return nodeType;
            }
            return null;
        }

        @JsonCreator
        public static NodeType create(String s) {
            if (s == null || s.length() == 0) return null;
            for (NodeType nodeType: NodeType.values()) {
                if (nodeType.value == s.charAt(0))
                    return nodeType;
            }
            return null;
        }

    }

    /**
     * Flag indicating status of node
     * - 'A' - active
     * - 'D' - deleted
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeType", length=1)
    @Convert(converter = NodeTypeConverter.class)
    public NodeType getType() {
        return m_type;
    }

    /**
     * <p>setType</p>
     *
     * @param nodetype a {@link java.lang.String} object.
     */
    public void setType(NodeType nodetype) {
        m_type = nodetype;
    }

    /**
     * SNMP MIB-2 system.sysObjectID.0
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeSysOID", length=256)
    public String getSysObjectId() {
        return m_sysObjectId;
    }

    /**
     * <p>setSysObjectId</p>
     *
     * @param nodesysoid a {@link java.lang.String} object.
     */
    public void setSysObjectId(String nodesysoid) {
        m_sysObjectId = nodesysoid;
    }

    /**
     * SNMP MIB-2 system.sysName.0
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeSysName", length=256)
    public String getSysName() {
        return m_sysName;
    }

    /**
     * <p>setSysName</p>
     *
     * @param nodesysname a {@link java.lang.String} object.
     */
    public void setSysName(String nodesysname) {
        m_sysName = nodesysname;
    }

    @Transient
    public boolean getHasFlows() {
        if (OnmsSnmpInterface.INGRESS_AND_EGRESS_REQUIRED) {
            return getHasIngressFlows() && getHasEgressFlows();
        } else {
            return getHasIngressFlows() || getHasEgressFlows();
        }
    }

    @Transient
    public boolean getHasIngressFlows() {
        if (m_lastIngressFlow == null) {
            return false;
        }
        return (System.currentTimeMillis() - m_lastIngressFlow.getTime()) / 1000 < OnmsSnmpInterface.MAX_FLOW_AGE;
    }

    @Transient
    public boolean getHasEgressFlows() {
        if (m_lastEgressFlow == null) {
            return false;
        }
        return (System.currentTimeMillis() - m_lastEgressFlow.getTime()) / 1000 < OnmsSnmpInterface.MAX_FLOW_AGE;
    }

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="last_ingress_flow")
    public Date getLastIngressFlow() {
        return m_lastIngressFlow;
    }

    public void setLastIngressFlow(Date lastIngressFlow) {
        this.m_lastIngressFlow = lastIngressFlow;
    }

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="last_egress_flow")
    public Date getLastEgressFlow() {
        return m_lastEgressFlow;
    }

    public void setLastEgressFlow(Date lastEgressFlow) {
        this.m_lastEgressFlow = lastEgressFlow;
    }

    /**
     * SNMP MIB-2 system.sysDescr.0
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeSysDescription", length=256)
    public String getSysDescription() {
        return m_sysDescription;
    }

    /**
     * <p>setSysDescription</p>
     *
     * @param nodesysdescription a {@link java.lang.String} object.
     */
    public void setSysDescription(String nodesysdescription) {
        m_sysDescription = nodesysdescription;
    }

    /**
     * SNMP MIB-2 system.sysLocation.0
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeSysLocation", length=256)
    public String getSysLocation() {
        return m_sysLocation;
    }

    /**
     * <p>setSysLocation</p>
     *
     * @param nodesyslocation a {@link java.lang.String} object.
     */
    public void setSysLocation(String nodesyslocation) {
        m_sysLocation = nodesyslocation;
    }

    /**
     * SNMP MIB-2 system.sysContact.0
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeSysContact", length=256)
    public String getSysContact() {
        return m_sysContact;
    }

    /**
     * <p>setSysContact</p>
     *
     * @param nodesyscontact a {@link java.lang.String} object.
     */
    public void setSysContact(String nodesyscontact) {
        m_sysContact = nodesyscontact;
    }

    /**
     * User-friendly name associated with the node.
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeLabel", length=256, nullable=false)
    public String getLabel() {
        return m_label;
    }

    /**
     * <p>setLabel</p>
     *
     * @param nodelabel a {@link java.lang.String} object.
     */
    public void setLabel(final String nodelabel) {
        if (m_label != null && m_oldLabel == null && !m_label.equals(nodelabel)) {
            m_oldLabel = m_label;
        }
        m_label = nodelabel;
    }

    public enum NodeLabelSource {
        /**
         * Label source set by user
         */
        USER('U'),

        /**
         * Label source set by netbios
         */
        NETBIOS('N'),

        /**
         * Label source set by hostname
         */
        HOSTNAME('H'),

        /**
         * Label source set by SNMP sysname
         */
        SYSNAME('S'),

        /**
         * Label source set by IP Address
         */
        ADDRESS('A'),

        /**
         * Label source unset/unknown
         */
        UNKNOWN(' ');

        private final char value;

        NodeLabelSource(char c) {
            value = c;
        }

        @JsonValue
        public char value() {
            return value;
        }

        @JsonCreator
        static NodeLabelSource create(String s) {
            if (s == null || s.length() == 0) return null;
            for (NodeLabelSource src : NodeLabelSource.values()) {
                if (src.value == s.charAt(0)) return src;
            }
            return null;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    /**
     * Flag indicating source of nodeLabel
     * - 'U' = user defined
     * - 'H' = IP hostname
     * - 'S' = sysName
     * - 'A' = IP address
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeLabelSource", length=1)
    @Convert(converter = NodeLabelSourceConverter.class)
    public NodeLabelSource getLabelSource() {
        return m_labelSource;
    }

    /**
     * <p>setLabelSource</p>
     *
     * @param nodelabelsource a {@link java.lang.String} object.
     */
    public void setLabelSource(final NodeLabelSource nodelabelsource) {
        if (m_labelSource != nodelabelsource && m_labelSource != null && m_oldLabelSource == null) {
            m_oldLabelSource = m_labelSource;
        }
        m_labelSource = nodelabelsource;
    }

    /**
     * NetBIOS workstation name associated with the node.
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeNetBIOSName", length=16)
    public String getNetBiosName() {
        return m_netBiosName;
    }

    /**
     * <p>setNetBiosName</p>
     *
     * @param nodenetbiosname a {@link java.lang.String} object.
     */
    public void setNetBiosName(String nodenetbiosname) {
        m_netBiosName = nodenetbiosname;
    }

    /**
     * NetBIOS domain name associated with the node.
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="nodeDomainName", length=16)
    public String getNetBiosDomain() {
        return m_netBiosDomain;
    }

    /**
     * <p>setNetBiosDomain</p>
     *
     * @param nodedomainname a {@link java.lang.String} object.
     */
    public void setNetBiosDomain(String nodedomainname) {
        m_netBiosDomain = nodedomainname;
    }

    /**
     * Operating system running on the node.
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="operatingSystem", length=64)
    public String getOperatingSystem() {
        return m_operatingSystem;
    }

    /**
     * <p>setOperatingSystem</p>
     *
     * @param operatingsystem a {@link java.lang.String} object.
     */
    public void setOperatingSystem(String operatingsystem) {
        m_operatingSystem = operatingsystem;
    }

    /**
     * Date and time of last Capsd scan.
     *
     * @return a {@link java.util.Date} object.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="lastCapsdPoll")
    public Date getLastCapsdPoll() {
        return m_lastCapsdPoll;
    }

    /**
     * <p>setLastCapsdPoll</p>
     *
     * @param lastcapsdpoll a {@link java.util.Date} object.
     */
    public void setLastCapsdPoll(Date lastcapsdpoll) {
        m_lastCapsdPoll = lastcapsdpoll;
    }

    /**
     * <p>getForeignId</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="foreignId")
    public String getForeignId() {
        return m_foreignId;
    }

    /**
     * <p>setForeignId</p>
     *
     * @param foreignId a {@link java.lang.String} object.
     */
    public void setForeignId(String foreignId) {
        m_foreignId = foreignId;
    }

    /**
     * <p>getForeignSource</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Column(name="foreignSource")
    public String getForeignSource() {
        return m_foreignSource;
    }

    /**
     * <p>setForeignSource</p>
     *
     * @param foreignSource a {@link java.lang.String} object.
     */
    public void setForeignSource(String foreignSource) {
        m_foreignSource = foreignSource;
    }

    /**
     * Monitoring location that this node is located in.
     */
    @ManyToOne(optional=false, fetch=FetchType.LAZY)
    @JoinColumn(name="location")
    public OnmsMonitoringLocation getLocation() {
        return m_location;
    }

    /**
     * Set the monitoring location that this node is located in.
     */
    public void setLocation(OnmsMonitoringLocation location) {
        m_location = location;
    }

    /**
     * The assert record associated with this node
     *
     * @return a {@link org.opennms.netmgt.model.OnmsAssetRecord} object.
     */
    // OnmsAssetRecord is not yet migrated to Jakarta Persistence.
    @Transient
    public OnmsAssetRecord getAssetRecord() {
        return m_assetRecord;
    }

    /**
     * <p>setAssetRecord</p>
     *
     * @param asset a {@link org.opennms.netmgt.model.OnmsAssetRecord} object.
     */
    public void setAssetRecord(OnmsAssetRecord asset) {
        m_assetRecord = asset;
        if (m_assetRecord != null) {
            m_assetRecord.setNode(this);
        }
    }

    /**
     * <p>getPathElement</p>
     *
     * @return a {@link org.opennms.netmgt.model.PathElement} object.
     */
    @JsonIgnore
    // PathElement is not yet migrated to Jakarta Persistence (@Embeddable).
    @Transient
    public PathElement getPathElement() {
        return m_pathElement;
    }

    /**
     * <p>setPathElement</p>
     *
     * @param pathElement a {@link org.opennms.netmgt.model.PathElement} object.
     */
    public void setPathElement(PathElement pathElement) {
        m_pathElement = pathElement;
    }


    /**
     * The interfaces on this node
     *
     * @return a {@link java.util.Set} object.
     */
    @JsonIgnore
    @OneToMany(mappedBy="node",orphanRemoval=true)
    @org.hibernate.annotations.Cascade(org.hibernate.annotations.CascadeType.ALL)
    public Set<OnmsIpInterface> getIpInterfaces() {
        return m_ipInterfaces;
    }

    /**
     * <p>setIpInterfaces</p>
     *
     * @param ipinterfaces a {@link java.util.Set} object.
     */
    public void setIpInterfaces(Set<OnmsIpInterface> ipinterfaces) {
        m_ipInterfaces = ipinterfaces;
    }

    /**
     * <p>addIpInterface</p>
     *
     * @param iface a {@link org.opennms.netmgt.model.OnmsIpInterface} object.
     */
    public void addIpInterface(OnmsIpInterface iface) {
        iface.setNode(this);
        getIpInterfaces().add(iface);
    }

    public void removeIpInterface(final OnmsIpInterface iface) {
        getIpInterfaces().remove(iface);
    }

    /**
     * The information from the SNMP interfaces/ipAddrTables for the node
     *
     * @return a {@link java.util.Set} object.
     */
    @JsonIgnore
    @OneToMany(mappedBy="node",orphanRemoval=true)
    @org.hibernate.annotations.Cascade(org.hibernate.annotations.CascadeType.ALL)
    public Set<OnmsSnmpInterface> getSnmpInterfaces() {
        return m_snmpInterfaces;
    }

    /**
     * <p>setSnmpInterfaces</p>
     *
     * @param snmpinterfaces a {@link java.util.Set} object.
     */
    public void setSnmpInterfaces(Set<OnmsSnmpInterface> snmpinterfaces) {
        m_snmpInterfaces = snmpinterfaces;
    }

    /**
     * <p>getCategories</p>
     *
     * @return a {@link java.util.Set} object.
     */
    @ManyToMany(cascade={CascadeType.PERSIST, CascadeType.MERGE})
    @JoinTable(
               name="category_node",
               joinColumns={@JoinColumn(name="nodeId")},
               inverseJoinColumns={@JoinColumn(name="categoryId")}
            )
    public Set<OnmsCategory> getCategories() {
        return m_categories;
    }

    /**
     * <p>setCategories</p>
     *
     * @param categories a {@link java.util.Set} object.
     */
    public void setCategories(Set<OnmsCategory> categories) {
        m_categories = categories;
    }

    /**
     * <p>addCategory</p>
     *
     * @param category a {@link org.opennms.netmgt.model.OnmsCategory} object.
     * @return a boolean.
     */
    public boolean addCategory(OnmsCategory category) {
        return getCategories().add(category);
    }

    /**
     * <p>removeCategory</p>
     *
     * @param category a {@link org.opennms.netmgt.model.OnmsCategory} object.
     * @return a boolean.
     */
    public boolean removeCategory(OnmsCategory category) {
        return getCategories().remove(category);
    }

    /**
     * <p>hasCategory</p>
     *
     * @param categoryName a {@link java.lang.String} object.
     * @return a boolean.
     */
    public boolean hasCategory(String categoryName) {
        for(OnmsCategory category : getCategories()) {
            if (category.getName().equals(categoryName)) {
                return true;
            }
        }
        return false;
    }

    @Transient
    public Set<String> getRequisitionedCategories() {
        return m_requisitionedCategories;
    }

    public void setRequisitionedCategories(final Set<String> categories) {
        m_requisitionedCategories = new LinkedHashSet<>(categories);
    }

    public void addRequisitionedCategory(final String category) {
        m_requisitionedCategories.add(category);
    }

    public void removeRequisitionedCategory(final String category) {
        m_requisitionedCategories.remove(category);
    }

    @JsonIgnore
    // OnmsMetaData is not yet migrated to Jakarta Persistence (@Embeddable).
    @Transient
    public List<OnmsMetaData> getMetaData() {
        return m_metaData;
    }

    public Optional<OnmsMetaData> findMetaDataForContextAndKey(final String context, final String key) {
        return getMetaData().stream()
                .filter(m -> m.getContext().equals(context))
                .filter(m -> m.getKey().equals(key))
                .findFirst();
    }

    public void setMetaData(final List<OnmsMetaData> metaData) {
        m_metaData = metaData;
    }

    public void addMetaData(final String context, final String key, final String value) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        final Optional<OnmsMetaData> entry = getMetaData().stream()
            .filter(m -> m.getContext().equals(context))
            .filter(m -> m.getKey().equals(key))
            .findFirst();

        // Update the value if present, otherwise create a new entry
        if (entry.isPresent()) {
            entry.get().setValue(value);
        } else {
            getMetaData().add(new OnmsMetaData(context, key, value));
        }
    }

    public void removeMetaData(final String context, final String key) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(key);
        final Iterator<OnmsMetaData> iterator = getMetaData().iterator();

        while (iterator.hasNext()) {
            final OnmsMetaData onmsNodeMetaData = iterator.next();

            if (context.equals(onmsNodeMetaData.getContext()) && key.equals(onmsNodeMetaData.getKey())) {
                iterator.remove();
            }
        }
    }

    public void removeMetaData(final String context) {
        Objects.requireNonNull(context);
        final Iterator<OnmsMetaData> iterator = getMetaData().iterator();

        while (iterator.hasNext()) {
            final OnmsMetaData onmsNodeMetaData = iterator.next();

            if (context.equals(onmsNodeMetaData.getContext())) {
                iterator.remove();
            }
        }
    }

    @Transient
    public List<OnmsMetaData> getRequisitionedMetaData() {
        return m_requisitionedMetaData;
    }

    public void setRequisionedMetaData(final List<OnmsMetaData> requisitionedMetaData) {
        m_requisitionedMetaData = requisitionedMetaData;
    }

    public void addRequisionedMetaData(final OnmsMetaData onmsMetaData) {
        m_requisitionedMetaData.add(onmsMetaData);
    }

    /**
     * <p>toString</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
        .add("id", m_id)
        .add("location", m_location == null ? null : m_location.getLocationName())
        .add("foreignSource", m_foreignSource)
        .add("foreignId", m_foreignId)
        .add("labelSource", m_labelSource == null ? null : m_labelSource.toString())
        .add("label", m_label)
        .add("parent.id", getParent() == null ? null : getParent().getId())
        .add("createTime", m_createTime)
        .add("sysObjectId", m_sysObjectId)
        .add("sysName", m_sysName)
        .add("sysDescription", m_sysDescription)
        .add("sysLocation", m_sysLocation)
        .add("sysContact", m_sysContact)
        .add("type", m_type == null ? null : m_type.toString())
        .add("operatingSystem", m_operatingSystem)
        .add("lastIngressFlow", m_lastIngressFlow)
        .add("lastEgressFlow", m_lastEgressFlow)
        .toString();
    }

    /** {@inheritDoc} */
    @Override
    public void visit(EntityVisitor visitor) {
        visitor.visitNode(this);

        for (OnmsIpInterface iface : getIpInterfaces()) {
            iface.visit(visitor);
        }

        for (OnmsSnmpInterface snmpIface : getSnmpInterfaces()) {
            snmpIface.visit(visitor);
        }

        visitor.visitNodeComplete(this);
    }

    /**
     * <p>addSnmpInterface</p>
     *
     * @param snmpIface a {@link org.opennms.netmgt.model.OnmsSnmpInterface} object.
     */
    public void addSnmpInterface(OnmsSnmpInterface snmpIface) {
        snmpIface.setNode(this);
        getSnmpInterfaces().add(snmpIface);
    }

    /**
     * <p>isDown</p>
     *
     * @return a boolean.
     */
    @Transient
    public boolean isDown() {
        boolean down = true;
        for (OnmsIpInterface ipIf : m_ipInterfaces) {
            if (!ipIf.isDown()) {
                return !down;
            }
        }
        return down;
    }

    /**
     * <p>getSnmpInterfaceWithIfIndex</p>
     *
     * @param ifIndex a int.
     * @return a {@link org.opennms.netmgt.model.OnmsSnmpInterface} object.
     */
    @Transient
    @JsonIgnore
    public OnmsSnmpInterface getSnmpInterfaceWithIfIndex(int ifIndex) {
        for (OnmsSnmpInterface dbSnmpIface : getSnmpInterfaces()) {
            if (dbSnmpIface.getIfIndex().equals(ifIndex)) {
                return dbSnmpIface;
            }
        }
        return null;
    }

    /**
     * <p>getIpInterfaceByIpAddress</p>
     *
     * @param ipAddress a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.model.OnmsIpInterface} object.
     */
    public OnmsIpInterface getIpInterfaceByIpAddress(String ipAddress) {
        return getIpInterfaceByIpAddress(InetAddressUtils.getInetAddress(ipAddress));
    }

    /**
     * <p>getIpInterfaceByIpAddress</p>
     *
     * @param ipAddress a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.model.OnmsIpInterface} object.
     */
    public OnmsIpInterface getIpInterfaceByIpAddress(InetAddress ipAddress) {
        for (OnmsIpInterface iface : getIpInterfaces()) {
            if (ipAddress.equals(iface.getIpAddress())) {
                return iface;
            }
        }
        return null;
    }

    /**
     * <p>compareTo</p>
     *
     * @param o a {@link org.opennms.netmgt.model.OnmsNode} object.
     * @return a int.
     */
    @Override
    public int compareTo(OnmsNode o) {
        String compareLabel = "";
        Integer compareId = 0;

        if (o != null) {
            compareLabel = o.getLabel();
            compareId = o.getId();
        }

        int returnval = this.getLabel().compareToIgnoreCase(compareLabel);
        if (returnval == 0) {
            return this.getId().compareTo(compareId);
        } else {
            return returnval;
        }
    }

    /**
     * <p>getPrimaryInterface</p>
     *
     * This function should be kept similar to {@link IpInterfaceDao#findPrimaryInterfaceByNodeId()}.
     *
     * @return a {@link org.opennms.netmgt.model.OnmsIpInterface} object.
     */
    @Transient
    @JsonIgnore
    public OnmsIpInterface getPrimaryInterface() {
        List<OnmsIpInterface> primaryInterfaces = new ArrayList<>();
        for(OnmsIpInterface iface : getIpInterfaces()) {
            if (PrimaryType.PRIMARY.equals(iface.getIsSnmpPrimary())) {
                primaryInterfaces.add(iface);
            }
        }
        if (primaryInterfaces.size() < 1) {
            return null;
        } else {
            if (primaryInterfaces.size() > 1) {
                // Sort the list by the last capabilities scan time so that we return the most recent value
                Collections.sort(primaryInterfaces, new Comparator<OnmsIpInterface>() {
                    @Override
                    public int compare(OnmsIpInterface o1, OnmsIpInterface o2) {
                        if (o1 == null) {
                            if (o2 == null) {
                                return 0;
                            } else {
                                return -1; // Put nulls at the end of the list
                            }
                        } else {
                            if (o2 == null) {
                                return 1; // Put nulls at the end of the list
                            } else {
                                if (o1.getIpLastCapsdPoll() == null) {
                                    if (o2.getIpLastCapsdPoll() == null) {
                                        return 0;
                                    } else {
                                        return 1; // Descending order
                                    }
                                } else {
                                    if (o2.getIpLastCapsdPoll() == null) {
                                        return -1; // Descending order
                                    } else {
                                        // Reverse the comparison so that we get a descending order
                                        return o2.getIpLastCapsdPoll().compareTo(o1.getIpLastCapsdPoll());
                                    }
                                }
                            }
                        }
                    }
                });
                OnmsIpInterface retval = primaryInterfaces.iterator().next();
                LOG.warn("Multiple primary SNMP interfaces for node {}, returning most recently scanned interface: {}", m_id, retval.getInterfaceId());
                return retval;
            } else {
                return primaryInterfaces.iterator().next();
            }
        }
    }

    /**
     * <p>getInterfaceWithService</p>
     *
     * @param svcName a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.model.OnmsIpInterface} object.
     */
    @Transient
    @JsonIgnore
    public OnmsIpInterface getInterfaceWithService(String svcName) {
        for(OnmsIpInterface iface : getIpInterfaces()) {
            if (iface.getMonitoredServiceByServiceType(svcName) != null) {
                return iface;
            }
        }
        return null;
    }

    /**
     * <p>getInterfacesWithService</p>
     *
     * @param svcName a {@link java.lang.String} object.
     * @return a List of {@link org.opennms.netmgt.model.OnmsIpInterface} objects.
     */
    @Transient
    @JsonIgnore
    public List<OnmsIpInterface> getInterfacesWithService(String svcName) {
        List<OnmsIpInterface> ipInterfaces = new ArrayList<>();
        for(OnmsIpInterface iface : getIpInterfaces()) {
            if (iface.getMonitoredServiceByServiceType(svcName) != null) {
                ipInterfaces.add(iface);
            }
        }
        return ipInterfaces;
    }

    @Transient
    @JsonIgnore
    public OnmsIpInterface getInterfaceWithAddress(final InetAddress addr) {
        if (addr == null) return null;
        for(final OnmsIpInterface iface : getIpInterfaces()) {
            if (addr.equals(iface.getIpAddress())) {
                return iface;
            }
        }
        return null;
    }

    /**
     * <p>getCriticalInterface</p>
     *
     * @return a {@link org.opennms.netmgt.model.OnmsIpInterface} object.
     */
    @Transient
    @JsonIgnore
    public OnmsIpInterface getCriticalInterface() {

        OnmsIpInterface critIface = getPrimaryInterface();
        if (critIface != null) {
            return critIface;
        }

        return getInterfaceWithService("ICMP");

    }

    /**
     * <p>mergeAgentAttributes</p>
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     */
    public void mergeAgentAttributes(OnmsNode scannedNode) {
        if (hasNewValue(scannedNode.getSysContact(), getSysContact())) {
            setSysContact(scannedNode.getSysContact());
        }

        if (hasNewValue(scannedNode.getSysDescription(), getSysDescription())) {
            setSysDescription(scannedNode.getSysDescription());
        }

        if (hasNewValue(scannedNode.getSysLocation(), getSysLocation())) {
            setSysLocation(scannedNode.getSysLocation());
        }

        if (hasNewValue(scannedNode.getSysName(), getSysName())) {
            setSysName(scannedNode.getSysName());
        }

        if (hasNewValue(scannedNode.getSysObjectId(), getSysObjectId())) {
            setSysObjectId(scannedNode.getSysObjectId());
        }
    }

    /**
     * <p>mergeNodeAttributes</p>
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     */
    public void mergeNodeAttributes(final OnmsNode scannedNode, final EventForwarder eventForwarder) {
        final String scannedLabel = scannedNode.getLabel();

        boolean send = false;

        if (m_oldLabel != null || m_oldLabelSource != null) {
            send = true;
        } else if (hasNewValue(scannedLabel, getLabel())) {
            m_oldLabel = getLabel();
            m_oldLabelSource = getLabelSource();
            send = true;
        }

        if (send) {
            LOG.debug("mergeNodeAttributes(): sending NODE_LABEL_CHANGED_EVENT_UEI");
            // Create a NODE_LABEL_CHANGED_EVENT_UEI event
            final EventBuilder bldr = new NodeLabelChangedEventBuilder("OnmsNode.mergeNodeAttributes");

            bldr.setNodeid(scannedNode.getId());
            bldr.setHost(InetAddressUtils.getLocalHostAddressAsString());

            if (m_oldLabel != null) {
                bldr.addParam(EventConstants.PARM_OLD_NODE_LABEL, m_oldLabel);
                if (m_oldLabelSource != null) {
                    bldr.addParam(EventConstants.PARM_OLD_NODE_LABEL_SOURCE, m_oldLabelSource.toString());
                }
            }

            if (scannedLabel != null) {
                bldr.addParam(EventConstants.PARM_NEW_NODE_LABEL, scannedLabel);
                if (scannedNode.getLabelSource() != null) {
                    bldr.addParam(EventConstants.PARM_NEW_NODE_LABEL_SOURCE, scannedNode.getLabelSource().toString());
                }
            }

            m_oldLabel = null;
            m_oldLabelSource = null;

            eventForwarder.sendNow(bldr.getEvent());

            // Update the node label value
            m_label = scannedLabel;
        } else {
            LOG.debug("mergeNodeAttributes(): skipping event.");
        }

        if (hasNewValue(scannedNode.getLocation(), getLocation())) {
            setLocation(scannedNode.getLocation());
        }

        if (hasNewValue(scannedNode.getForeignSource(), getForeignSource())) {
            setForeignSource(scannedNode.getForeignSource());
        }

        if (hasNewValue(scannedNode.getForeignId(), getForeignId())) {
            setForeignId(scannedNode.getForeignId());
        }

        if (hasNewValue(scannedNode.getLabelSource(), getLabelSource())) {
            setLabelSource(scannedNode.getLabelSource());
        }

        if (hasNewValue(scannedNode.getNetBiosName(), getNetBiosDomain())) {
            setNetBiosName(scannedNode.getNetBiosDomain());
        }

        if (hasNewValue(scannedNode.getNetBiosDomain(), getNetBiosDomain())) {
            setNetBiosDomain(scannedNode.getNetBiosDomain());
        }

        if (hasNewValue(scannedNode.getOperatingSystem(), getOperatingSystem())) {
            setOperatingSystem(scannedNode.getOperatingSystem());
        }

        mergeAgentAttributes(scannedNode);

        mergeAdditionalCategories(scannedNode);
    }

    /**
     * <p>mergeAdditionalCategories</p>
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     */
    private void mergeAdditionalCategories(OnmsNode scannedNode) {
        getCategories().addAll(scannedNode.getCategories());
    }

    /**
     * <p>mergeSnmpInterfaces</p>
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     * @param deleteMissing a boolean.
     */
    public void mergeSnmpInterfaces(OnmsNode scannedNode, boolean deleteMissing) {

        // we need to skip this step if there is an indication that snmp data collection failed
        if (scannedNode.getSnmpInterfaces().size() == 0) {
            // we assume here that snmp collection failed and we don't update the snmp data
            return;
        }


        // Build map of ifIndices to scanned SnmpInterfaces
        Map<Integer, OnmsSnmpInterface> scannedInterfaceMap = new HashMap<Integer, OnmsSnmpInterface>();
        for (OnmsSnmpInterface snmpIface : scannedNode.getSnmpInterfaces()) {
            if (snmpIface.getIfIndex() != null) {
                scannedInterfaceMap.put(snmpIface.getIfIndex(), snmpIface);
            }
        }

        // for each interface on existing node...
        for (Iterator<OnmsSnmpInterface> it = getSnmpInterfaces().iterator(); it.hasNext();) {

            OnmsSnmpInterface iface = it.next();
            OnmsSnmpInterface imported = scannedInterfaceMap.get(iface.getIfIndex());

            // remove it since there is no corresponding scanned interface
            if (imported == null) {
                if (deleteMissing) {
                    it.remove();
                    scannedInterfaceMap.remove(iface.getIfIndex());
                }
            } else {
                // merge the data from the corresponding scanned interface
                iface.mergeSnmpInterfaceAttributes(imported);
                scannedInterfaceMap.remove(iface.getIfIndex());
            }

        }

        // for any scanned interface that was not found on the node add it the database
        for (OnmsSnmpInterface snmpIface : scannedInterfaceMap.values()) {
            addSnmpInterface(snmpIface);
        }
    }

    /**
     * <p>mergeIpInterfaces</p>
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     * @param eventForwarder a {@link org.opennms.netmgt.events.api.EventForwarder} object.
     * @param deleteMissing a boolean.
     */
    public void mergeIpInterfaces(OnmsNode scannedNode, EventForwarder eventForwarder, boolean deleteMissing) {
        OnmsIpInterface oldPrimaryInterface = null;
        OnmsIpInterface scannedPrimaryIf = null;
        // build a map of ipAddrs to ipInterfaces for the scanned node
        Map<InetAddress, OnmsIpInterface> ipInterfaceMap = new HashMap<InetAddress, OnmsIpInterface>();
        for (OnmsIpInterface iface : scannedNode.getIpInterfaces()) {
            if(scannedPrimaryIf == null && iface.isPrimary()){
                scannedPrimaryIf = iface;
            }else if(iface.isPrimary()){
                iface.setIsSnmpPrimary(PrimaryType.SECONDARY);
            }

            ipInterfaceMap.put(iface.getIpAddress(), iface);
        }

        // for each ipInterface from the database
        for (Iterator<OnmsIpInterface> it = getIpInterfaces().iterator(); it.hasNext();) {
            OnmsIpInterface dbIface = it.next();
            // find the corresponding scanned Interface
            OnmsIpInterface scannedIface = ipInterfaceMap.get(dbIface.getIpAddress());

            // if we can't find a scanned interface remove from the database
            if (scannedIface == null) {
                if (deleteMissing) {
                    it.remove();
                    dbIface.visit(new DeleteEventVisitor(eventForwarder));
                }else if(scannedPrimaryIf != null && dbIface.isPrimary()){
                    dbIface.setIsSnmpPrimary(PrimaryType.SECONDARY);
                    oldPrimaryInterface = dbIface;

                }
            } else {
                // else update the database with scanned info
                dbIface.mergeInterface(scannedIface, eventForwarder, deleteMissing);
                if(scannedPrimaryIf != null && dbIface.isPrimary() && scannedPrimaryIf != scannedIface){
                    dbIface.setIsSnmpPrimary(PrimaryType.SECONDARY);
                    oldPrimaryInterface = dbIface;
                }
            }

            // now remove the interface from the map to indicate it was processed
            ipInterfaceMap.remove(dbIface.getIpAddress());
        }


        // for any remaining scanned interfaces, add them to the database
        for (OnmsIpInterface iface : ipInterfaceMap.values()) {
            addIpInterface(iface);
            if (iface.getIfIndex() != null) {
                iface.setSnmpInterface(getSnmpInterfaceWithIfIndex(iface.getIfIndex()));
            }
            iface.visit(new AddEventVisitor(eventForwarder));
        }

        if(oldPrimaryInterface != null && scannedPrimaryIf != null){
            EventBuilder bldr = new EventBuilder(EventConstants.PRIMARY_SNMP_INTERFACE_CHANGED_EVENT_UEI, "Provisiond");
            if (scannedPrimaryIf.getNode() != null) {
                bldr.setNodeid(scannedPrimaryIf.getNode().getId().longValue());
            }
            bldr.setInterface(scannedPrimaryIf.getIpAddress());
            bldr.setService("SNMP");
            bldr.addParam(EventConstants.PARM_OLD_PRIMARY_SNMP_ADDRESS, InetAddressUtils.str(oldPrimaryInterface.getIpAddress()));
            bldr.addParam(EventConstants.PARM_NEW_PRIMARY_SNMP_ADDRESS, InetAddressUtils.str(scannedPrimaryIf.getIpAddress()));

            eventForwarder.sendNow(bldr.getEvent());
        }
    }

    /**
     * <p>mergeCategorySet</p>
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     */
    public void mergeCategorySet(OnmsNode scannedNode) {
        if (!getCategories().equals(scannedNode.getCategories())) {
            setCategories(scannedNode.getCategories());
        }
    }

    /**
     * Truly merges the node's assert record
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     */
    public void mergeAssets(OnmsNode scannedNode) {
        this.getAssetRecord().mergeRecord(scannedNode.getAssetRecord());
    }

    /**
     * Truly merges the node's meta-data
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     */
    public void mergeMetaData(OnmsNode scanned) {
        if (!getMetaData().equals(scanned.getMetaData())) {
            setMetaData(scanned.getMetaData());
        }
    }

    /**
     * Simply replaces the current asset record with the new record
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     */
    public void replaceCurrentAssetRecord(OnmsNode scannedNode) {
        scannedNode.getAssetRecord().setId(this.getAssetRecord().getId());
        scannedNode.setId(this.m_id);  //just in case
        this.setAssetRecord(scannedNode.getAssetRecord());
    }

    /**
     * <p>mergeNode</p>
     *
     * @param scannedNode a {@link org.opennms.netmgt.model.OnmsNode} object.
     * @param eventForwarder a {@link org.opennms.netmgt.events.api.EventForwarder} object.
     * @param deleteMissing a boolean.
     */
    public void mergeNode(OnmsNode scannedNode, EventForwarder eventForwarder, boolean deleteMissing) {

        mergeNodeAttributes(scannedNode, eventForwarder);

        mergeSnmpInterfaces(scannedNode, deleteMissing);

        mergeIpInterfaces(scannedNode, eventForwarder, deleteMissing);

        mergeCategorySet(scannedNode);

        mergeAssets(scannedNode);

        mergeMetaData(scannedNode);
    }

    @Transient
    @JsonIgnore
    public boolean containsService(final InetAddress addr, final String service) {
        final OnmsIpInterface iface = getInterfaceWithAddress(addr);
        if (iface != null) {
            final OnmsMonitoredService svc = iface.getMonitoredServiceByServiceType(service);
            return svc != null;
        }
        return false;
    }

    @Transient
    @JsonIgnore
    public boolean containsInterface(final InetAddress addr) {
        return (getInterfaceWithAddress(addr) != null);
    }

}
