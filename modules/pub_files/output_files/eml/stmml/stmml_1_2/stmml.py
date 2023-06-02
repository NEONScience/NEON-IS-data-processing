from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Union

__NAMESPACE__ = "http://www.xml-cml.org/schema/stmml-1.2"


@dataclass
class Action:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">An action which might occur in scientific data or
    narrative.</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>An action which might occur in scientific data or narrative.
    The definition is deliberately vague, intending to collect examples of
    possible usage. Thus an action could be addition of materials,
    measurement, application of heat or radiation.
    The content model is unrestricted. <ns1:tt>action</ns1:tt> iself is
    normally a child of <ns1:a href="#el.actionList">actionList</ns1:a></ns1:p>
    <ns1:p>The start, end and duration attributes should be interpreted as
    </ns1:p>
    <ns1:ul>
    <ns1:li>XSD dateTimes and XSD durations. This allows precise recording of
    time of day, etc, or duration
    after start of actionList. A <ns1:tt>convention="xsd"</ns1:tt> attribute should be used
    to enforce XSD.</ns1:li>
    <ns1:li>a numerical value, with a units attribute linked to a dictionary.</ns1:li>
    <ns1:li>a human-readable string (unlikely to be machine processable)</ns1:li>
    </ns1:ul>
    <ns1:p><ns1:tt>startCondition</ns1:tt> and <ns1:tt>endCondition</ns1:tt>
    values are not constrained, which allows XSL-like <ns1:tt>test</ns1:tt> attribute values.
    The semantics of the conditions are yet to be defined and at present are simply
    human readable.
    </ns1:p>
    <ns1:p>The order of the <ns1:tt>action</ns1:tt> elements in the document may, but will not always, define
    the order that they actually occur in.</ns1:p>
    <ns1:p>A delay can be shown by an <ns1:tt>action</ns1:tt> with no content. Repeated actions or
    actionLists are indicated through the count attribute.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;actionList title="boiling two eggs for breakfast"&gt;
    &lt;!-- start cooking at 9am --&gt;
    &lt;action title="turn on heat" start="T09:00:00" convention="xsd"/&gt;
    &lt;!-- human readable description of time to start action --&gt;
    &lt;action title="put egg into pan" startCondition="water is boiling" count="2"/&gt;
    &lt;!-- the duration is expressed in ISO8601 format --&gt;
    &lt;action title="boil eggs for 4 minutes" duration="4" units="units:min"/&gt;
    &lt;!-- action immediately follows last action --&gt;
    &lt;action title="remove egg from pan" count="1"/&gt;
    &lt;action title="boil second egg for a bit longer" duration="about half a minute"/&gt;
    &lt;!-- action immediately follows last action --&gt;
    &lt;action title="remove egg from pan" count="1"/&gt;
    &lt;/actionList&gt;
    </ns1:pre>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;actionList title="preparation of silanols"&gt;
    &lt;p&gt;This is a conversion of a chemical synthesis to STM-ML. We
    have deliberately not marked up the chemistry in this example!&lt;/p&gt;
    &lt;action title="step2"&gt;
    &lt;p&gt;Take 1 mmol of the diol and dissolve in dioxan in
    &lt;object title="flask"&gt;
    &lt;scalar title="volume" units="units:ml"&gt;25&lt;/scalar&gt;
    &lt;/object&gt;
    &lt;/p&gt;
    &lt;/action&gt;
    &lt;action title="step2"&gt;
    &lt;p&gt;Place flask in water bath with magnetic stirrer&lt;/p&gt;
    &lt;/action&gt;
    &lt;!-- wait until certain condition --&gt;
    &lt;actionList endCondition="bath temperature stabilised"/&gt;
    &lt;action title="step3"&gt;
    &lt;p&gt;Add 0.5 ml 1 M H2SO4&lt;/p&gt;
    &lt;/action&gt;
    &lt;!-- carry out reaction --&gt;
    &lt;actionList endCondition="reaction complete; no diol spot remains on TLC"&gt;
    &lt;actionList title="check tlc"&gt;
    &lt;!-- wait for half an hour --&gt;
    &lt;action duration="half an hour"/&gt;
    &lt;action title="tlc"&gt;
    &lt;p&gt;extract solution and check diol spot on TLC&lt;/p&gt;
    &lt;/action&gt;
    &lt;/actionList&gt;
    &lt;/actionList&gt;
    &lt;!-- work up reaction --&gt;
    &lt;action title="step5"&gt;
    &lt;p&gt;Add 10 ml water to flask&lt;/p&gt;
    &lt;/action&gt;
    &lt;action title="step6"&gt;
    &lt;p&gt;Neutralize acid with 10% NaHCO3&lt;/p&gt;
    &lt;/action&gt;
    &lt;action title="step7" count="3"&gt;
    &lt;p&gt;Extract with 10ml ether&lt;/p&gt;
    &lt;/action&gt;
    &lt;action title="step8"&gt;
    &lt;p&gt;Combine ether layers&lt;/p&gt;
    &lt;/action&gt;
    &lt;action title="step9" count="2"&gt;
    &lt;p&gt;Wash ether with 10 ml water&lt;/p&gt;
    &lt;/action&gt;
    &lt;action title="step10"&gt;
    &lt;p&gt;Wash ether with 10 ml saturated NaCl&lt;/p&gt;
    &lt;/action&gt;
    &lt;action title="step11"&gt;
    &lt;p&gt;Dry over anhydrous Na2SO4 and remove solvent on rotary evaporator&lt;/p&gt;
    &lt;/action&gt;
    &lt;/actionList&gt;
    </ns1:pre>
    </ns1:div>

    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar start: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The start
        time</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>The start
        time in any allowable XSD representation of date, time or
        dateTime. This will normally be a clock time or date.</ns1:p>
        </ns1:div>
    :ivar start_condition:
    :ivar duration:
    :ivar end: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The end time</ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>The start time in any allowable XSD
        representation of date, time or dateTime. This will normally be
        a clock time or date.</ns1:p> </ns1:div>
    :ivar end_condition: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>The end
        condition</ns1:p> </ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>At present
        a human-readable string describing some condition when the ac
        tion should end. As XML develops it may be possible to add
        machine-processable semantics in this field.</ns1:p> </ns1:div>
    :ivar units: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">Units for the time
        attributes</ns1:div>
    :ivar count: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">Number of times the
        action should be repeated</ns1:div>
    :ivar type: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The type of the
        action; semantics are not controlled.</ns1:div>
    :ivar content:
    """
    class Meta:
        name = "action"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    start: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    start_condition: Optional[str] = field(
        default=None,
        metadata={
            "name": "startCondition",
            "type": "Attribute",
        }
    )
    duration: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    end: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    end_condition: Optional[str] = field(
        default=None,
        metadata={
            "name": "endCondition",
            "type": "Attribute",
        }
    )
    units: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    count: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "min_inclusive": 1,
            "max_inclusive": 999999999999,
        }
    )
    type: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        }
    )


class ActionListOrder(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


class AlternativeValue(Enum):
    SYNONYM = "synonym"
    QUASI_SYNONYM = "quasi-synonym"
    ACRONYM = "acronym"
    ABBREVIATION = "abbreviation"
    HOMONYM = "homonym"
    IDENTIFIER = "identifier"


@dataclass
class Appinfo:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">

    <ns1:p>A container similar to <ns1:tt>appinfo</ns1:tt> in XML Schema.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>A container for machine processable documentation for an entry.
    This is likely to be platform and/or language specific. It is possible
    that XSLT, RDF or XBL will emerge as generic languages</ns1:p>
    <ns1:p>See <ns1:a href="el.annotation">annotation</ns1:a> and <ns1:a href="el.documentation">documentation</ns1:a> for further information</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example"><ns1:p>An example in XSLT where an element <ns1:tt>foo</ns1:tt> calls a bespoke
    template</ns1:p>.
    <ns1:pre>
    &lt;s:appinfo
    xmlns:s="http://www.xml-cml.org/schema/core"
    xmlns="http://www.w3.org/1999/XSL/Transform"&gt;
    &lt;template match="foo"&gt;
    &lt;call-template name="processFoo"/&gt;
    &lt;/template&gt;
    &lt;/s:appinfo&gt;
    </ns1:pre></ns1:div>
    """
    class Meta:
        name = "appinfo"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    source: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        }
    )


class DataTypeType(Enum):
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">

    <ns1:p>an enumerated type for all builtin allowed dataTypes in STM</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p><ns1:tt>dataTypeType</ns1:tt> represents an enumeration of allowed dataTypes
    (at present identical with those in XML-Schemas (Part2- datatypes).
    This means that implementers should be able to use standard XMLSchema-based
    tools for validation without major implementation problems.
    </ns1:p>
    <ns1:p>It will often be used an an attribute on
    <ns1:a href="el.scalar">scalar</ns1:a>,
    <ns1:a href="el.array">array</ns1:a> or
    <ns1:a href="el.matrix">matrix</ns1:a>
    elements.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;list xmlns="http://www.xml-cml.org/schema/core"&gt;
    &lt;scalar dataType="xsd:boolean" title="she loves me"&gt;true&lt;/scalar&gt;
    &lt;scalar dataType="xsd:float" title="x"&gt;23.2&lt;/scalar&gt;
    &lt;scalar dataType="xsd:duration" title="egg timer"&gt;PM4&lt;/scalar&gt;
    &lt;scalar dataType="xsd:dateTime" title="current data and time"&gt;2001-02-01:00:30&lt;/scalar&gt;
    &lt;scalar dataType="xsd:time" title="wake up"&gt;06:00&lt;/scalar&gt;
    &lt;scalar dataType="xsd:date" title="where is it"&gt;1752-09-10&lt;/scalar&gt;
    &lt;scalar dataType="xsd:anyURI" title="CML site"&gt;http://www.xml-cml.org/&lt;/scalar&gt;
    &lt;scalar dataType="xsd:QName" title="CML atom"&gt;cml:atom&lt;/scalar&gt;
    &lt;scalar dataType="xsd:normalizedString" title="song"&gt;the mouse ran up the clock&lt;/scalar&gt;
    &lt;scalar dataType="xsd:language" title="UK English"&gt;en-GB&lt;/scalar&gt;
    &lt;scalar dataType="xsd:Name" title="atom"&gt;atom&lt;/scalar&gt;
    &lt;scalar dataType="xsd:ID" title="XML ID"&gt;_123&lt;/scalar&gt;
    &lt;scalar dataType="xsd:integer" title="the answer"&gt;42&lt;/scalar&gt;
    &lt;scalar dataType="xsd:nonPositiveInteger" title="zero"&gt;0&lt;/scalar&gt;
    &lt;/list&gt;
    </ns1:pre>
    </ns1:div>
    """
    XSD_STRING = "xsd:string"
    XSD_BOOLEAN = "xsd:boolean"
    XSD_FLOAT = "xsd:float"
    XSD_DOUBLE = "xsd:double"
    XSD_DECIMAL = "xsd:decimal"
    XSD_DURATION = "xsd:duration"
    XSD_DATE_TIME = "xsd:dateTime"
    XSD_TIME = "xsd:time"
    XSD_DATE = "xsd:date"
    XSD_G_YEAR_MONTH = "xsd:gYearMonth"
    XSD_G_YEAR = "xsd:gYear"
    XSD_G_MONTH_DAY = "xsd:gMonthDay"
    XSD_G_DAY = "xsd:gDay"
    XSD_G_MONTH = "xsd:gMonth"
    XSD_HEX_BINARY = "xsd:hexBinary"
    XSD_BASE64_BINARY = "xsd:base64Binary"
    XSD_ANY_URI = "xsd:anyURI"
    XSD_QNAME = "xsd:QName"
    XSD_NOTATION = "xsd:NOTATION"
    XSD_NORMALIZED_STRING = "xsd:normalizedString"
    XSD_TOKEN = "xsd:token"
    XSD_LANGUAGE = "xsd:language"
    XSD_IDREFS = "xsd:IDREFS"
    XSD_ENTITIES = "xsd:ENTITIES"
    XSD_NMTOKEN = "xsd:NMTOKEN"
    XSD_NMTOKENS = "xsd:NMTOKENS"
    XSD_NAME = "xsd:Name"
    XSD_NCNAME = "xsd:NCName"
    XSD_ID = "xsd:ID"
    XSD_IDREF = "xsd:IDREF"
    XSD_ENTITY = "xsd:ENTITY"
    XSD_INTEGER = "xsd:integer"
    XSD_NON_POSITIVE_INTEGER = "xsd:nonPositiveInteger"
    XSD_NEGATIVE_INTEGER = "xsd:negativeInteger"
    XSD_LONG = "xsd:long"
    XSD_INT = "xsd:int"
    XSD_SHORT = "xsd:short"
    XSD_BYTE = "xsd:byte"
    XSD_NON_NEGATIVE_INTEGER = "xsd:nonNegativeInteger"
    XSD_UNSIGNED_LONG = "xsd:unsignedLong"
    XSD_UNSIGNED_INT = "xsd:unsignedInt"
    XSD_UNSIGNED_SHORT = "xsd:unsignedShort"
    XSD_UNSIGNED_BYTE = "xsd:unsignedByte"
    XSD_POSITIVE_INTEGER = "xsd:positiveInteger"


@dataclass
class Definition:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">

    <ns1:p>The definition for a dictionary entry, scientific units, etc.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>The definition should be a short nounal phrase definining the
    subject of the entry. Definitions should not include commentary, implementations,
    equations or formulae (unless the subject is one of these) or examples. The
    <ns1:tt>description</ns1:tt> element can be used for these.</ns1:p>
    <ns1:p>The definition can be in any markup language, but normally XHTML will be used,
    perhaps with links to other XML namespaces such as CML for chemistry.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:em>From the IUPAC Dictionary of Medicinal Chemistry</ns1:em>
    <ns1:br/>
    <ns1:pre>
    &lt;entry id="a7" term="Allosteric enzyme"&gt;
    &lt;definition&gt;An &lt;a href="#e3"&gt;enzyme&lt;/a&gt;
    that contains a region to which small, regulatory molecules
    ("effectors") may bind in addition to and separate from the
    substrate binding site and thereby affect the catalytic
    activity.
    &lt;/definition&gt;
    &lt;description&gt;On binding the effector, the catalytic activity of the
    &lt;strong&gt;enzyme&lt;/strong&gt; towards the substrate may be enhanced, in
    which case the effector is an activator, or reduced, in which case
    it is a de-activator or inhibitor.
    &lt;/description&gt;
    &lt;/entry&gt;
    </ns1:pre>
    </ns1:div>

    :ivar source: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An attribute linking
        to the source of the information</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>A simple way of adding metadata to a
        piece of information. Likely to be fragile since the URI may
        disappear.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;list&gt; &lt;definition
        source="foo.html#a3"&gt;An animal with four
        legs&lt;/definition&gt; &lt;definition
        source="http://www.foo.com/index.html"&gt; An animal with six
        legs&lt;/definition&gt; &lt;/list&gt; </ns1:pre> </ns1:div>
    :ivar content:
    """
    class Meta:
        name = "definition"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    source: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        }
    )


@dataclass
class Description:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">Descriptive information in a dictionary entry,
    etc.</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>Entries should have at least one separate <ns1:a href="el.definition">definition</ns1:a>s.
    <ns1:tt>description</ns1:tt> is then used for most of the other information, including
    examples. The <ns1:tt>class</ns1:tt> attribute has an uncontrolled vocabulary and
    can be used to clarify the purposes of the <ns1:tt>description</ns1:tt>
    elements.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:em>From IUPAC Dictionary of Medicinal Chemistry</ns1:em>
    <ns1:pre>
    &lt;entry id="a7" term="Allosteric enzyme"&gt;
    &lt;definition&gt;An &lt;a href="#e3"&gt;enzyme&lt;/a&gt;
    that contains a region to which small, regulatory molecules
    ("effectors") may bind in addition to and separate from the
    substrate binding site and thereby affect the catalytic
    activity.
    &lt;/definition&gt;
    &lt;description&gt;On binding the effector, the catalytic activity of the
    &lt;strong&gt;enzyme&lt;/strong&gt; towards the substrate may be enhanced, in
    which case the effector is an activator, or reduced, in which case
    it is a de-activator or inhibitor.
    &lt;/description&gt;
    &lt;/entry&gt;
    </ns1:pre>
    </ns1:div>

    :ivar source: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An attribute linking
        to the source of the information</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>A simple way of adding metadata to a
        piece of information. Likely to be fragile since the URI may
        disappear.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;list&gt; &lt;definition
        source="foo.html#a3"&gt;An animal with four
        legs&lt;/definition&gt; &lt;definition
        source="http://www.foo.com/index.html"&gt; An animal with six
        legs&lt;/definition&gt; &lt;/list&gt; </ns1:pre> </ns1:div>
    :ivar class_value: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>The type
        of this information. This is not controlled, but examples might
        include:</ns1:p> <ns1:ul> <ns1:li>description</ns1:li>
        <ns1:li>summary</ns1:li> <ns1:li>note</ns1:li>
        <ns1:li>usage</ns1:li> <ns1:li>qualifier</ns1:li> </ns1:ul>
        </ns1:div>
    :ivar content:
    """
    class Meta:
        name = "description"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    source: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    class_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "class",
            "type": "Attribute",
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        }
    )


class DimensionType(Enum):
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">Allowed values for dimension Types (for
    quantities).</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>These are the 7 types prescribed by the SI system, together
    with the "dimensionless" type. We intend to be somewhat uncoventional
    and explore enhanced values of "dimensionless", such as "angle".
    This may be heretical, but we find the present system impossible to implement
    in many cases.</ns1:p>
    <ns1:p>Used for constructing entries in a dictionary of units</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;unitType id="energy" name="energy"&gt;
    &lt;dimension name="length"/&gt;
    &lt;dimension name="mass"/&gt;
    &lt;dimension name="time" power="-1"/&gt;
    &lt;/unitType&gt;
    </ns1:pre>
    </ns1:div>

    :cvar MASS:
    :cvar LENGTH:
    :cvar TIME:
    :cvar CURRENT:
    :cvar AMOUNT:
    :cvar LUMINOSITY:
    :cvar TEMPERATURE:
    :cvar DIMENSIONLESS:
    :cvar ANGLE: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An angle (formally
        dimensionless, but useful to have units).</ns1:div>
    """
    MASS = "mass"
    LENGTH = "length"
    TIME = "time"
    CURRENT = "current"
    AMOUNT = "amount"
    LUMINOSITY = "luminosity"
    TEMPERATURE = "temperature"
    DIMENSIONLESS = "dimensionless"
    ANGLE = "angle"


@dataclass
class Documentation:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">Documentation in the <ns1:a
    href="el.annotation">annotation</ns1:a> of an <ns1:a
    href="el.entry">entry</ns1:a></ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>A container similar to <ns1:tt>documentation</ns1:tt> in XML Schema.
    This is NOT part of the textual content of an entry but is designed to
    support the transformation of dictionary entrys into schemas for validation.
    This is experimental and should only be used for dictionaries, units, etc.
    One approach is to convert these into XML Schemas when the <ns1:tt>documentation</ns1:tt>
    and <ns1:tt>appinfo</ns1:tt> children will emerge in their correct position in the
    derived schema.</ns1:p>
    <ns1:p>Do NOT confuse documentation with the <ns1:a href="el.definition">definition</ns1:a>
    or the <ns1:a href="el.definition">definition</ns1:a> which are part of the content
    of the dictionary</ns1:p>
    <ns1:p>If will probably only be used when there is significant <ns1:a href="el.appinfo">appinfo</ns1:a>
    in the entry or where the entry defines an XSD-like datatype of an element in the document.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;stm:documentation id="source"
    xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
    Transcribed from IUPAC website
    &lt;/stm:documentation&gt;
    </ns1:pre>
    </ns1:div>

    :ivar source: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An attribute linking
        to the source of the information</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>A simple way of adding metadata to a
        piece of information. Likely to be fragile since the URI may
        disappear.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;list&gt; &lt;definition
        source="foo.html#a3"&gt;An animal with four
        legs&lt;/definition&gt; &lt;definition
        source="http://www.foo.com/index.html"&gt; An animal with six
        legs&lt;/definition&gt; &lt;/list&gt; </ns1:pre> </ns1:div>
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar content:
    """
    class Meta:
        name = "documentation"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    source: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        }
    )


class ErrorBasisType(Enum):
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">The basis of an error value</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>Errors in values can be of several types and this simpleType
    provides a small controlled vocabulary</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;scalar
    dataType="xsd:decimal"
    errorValue="1.0"
    errorBasis="observedStandardDeviation"
    title="body weight"
    dictRef="zoo:bodywt"
    units="units:g"&gt;34.3&lt;/scalar&gt;
    </ns1:pre>
    </ns1:div>
    """
    OBSERVED_RANGE = "observedRange"
    OBSERVED_STANDARD_DEVIATION = "observedStandardDeviation"
    OBSERVED_STANDARD_ERROR = "observedStandardError"
    ESTIMATED_STANDARD_DEVIATION = "estimatedStandardDeviation"
    ESTIMATED_STANDARD_ERROR = "estimatedStandardError"


class LinkType(Enum):
    """
    :cvar EXTENDED: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A container for
        locators</ns1:div>
    :cvar LOCATOR: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A link to an
        element</ns1:div>
    :cvar ARC: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A labelled
        link</ns1:div>
    """
    EXTENDED = "extended"
    LOCATOR = "locator"
    ARC = "arc"


@dataclass
class ListType:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A generic container with no implied semantics</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>
    A generic container with no implied semantics. It just contains
    things and can have attributes which bind conventions to it. It could often
    act as the root element in an STM document.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;list&gt;
    &lt;array title="animals"&gt;frog bear toad&lt;/array&gt;
    &lt;scalar title="weight" dataType="xsd:float"&gt;3.456&lt;/scalar&gt;
    &lt;/list&gt;
    </ns1:pre>
    </ns1:div>

    :ivar any_element:
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar type:
    """
    class Meta:
        name = "list"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    any_element: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    type: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )


class MatrixTypeValue(Enum):
    """
    :cvar RECTANGULAR:
    :cvar SQUARE:
    :cvar SQUARE_SYMMETRIC:
    :cvar SQUARE_ANTISYMMETRIC:
    :cvar DIAGONAL: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description">Symmetric.
        Elements are zero except on the diagonal</ns1:div>
    :cvar UPPER_TRIANGULAR: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description">Square. Elements
        are zero below the diagonal <ns1:pre> 1 2 3 4 0 3 5 6 0 0 4 8 0
        0 0 2 </ns1:pre></ns1:div>
    :cvar LOWER_TRIANGULAR: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description">Symmetric.
        Elements are zero except on the diagonal</ns1:div>
    :cvar UNITARY:
    :cvar ROW_EIGENVECTORS:
    :cvar ROTATION22:
    :cvar ROTATION_TRANSLATION32:
    :cvar HOMOGENEOUS33:
    :cvar ROTATION33:
    :cvar ROTATION_TRANSLATION43:
    :cvar HOMOGENEOUS44:
    """
    RECTANGULAR = "rectangular"
    SQUARE = "square"
    SQUARE_SYMMETRIC = "squareSymmetric"
    SQUARE_ANTISYMMETRIC = "squareAntisymmetric"
    DIAGONAL = "diagonal"
    UPPER_TRIANGULAR = "upperTriangular"
    LOWER_TRIANGULAR = "lowerTriangular"
    UNITARY = "unitary"
    ROW_EIGENVECTORS = "rowEigenvectors"
    ROTATION22 = "rotation22"
    ROTATION_TRANSLATION32 = "rotationTranslation32"
    HOMOGENEOUS33 = "homogeneous33"
    ROTATION33 = "rotation33"
    ROTATION_TRANSLATION43 = "rotationTranslation43"
    HOMOGENEOUS44 = "homogeneous44"


class MetadataType(Enum):
    """
    :cvar DC_COVERAGE: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">The extent or scope
        of the content of the resource.</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Coverage will typically include spatial
        location (a place name or geographic coordinates), temporal
        period (a period label, date, or date range) or jurisdiction
        (such as a named administrative entity). Recommended best
        practice is to select a value from a controlled vocabulary (for
        example, the Thesaurus of Geographic Names [TGN]) and that,
        where appropriate, named places or time periods be used in
        preference to numeric identifiers such as sets of coordinates or
        date ranges. </ns1:div>
    :cvar DC_DESCRIPTION: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">An account of the
        content of the resource.</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Description may include but is not limited
        to: an abstract, table of contents, reference to a graphical
        representation of content or a free-text account of the content.
        </ns1:div>
    :cvar DC_IDENTIFIER: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">An unambiguous
        reference to the resource within a given context.</ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Recommended best practice is to identify the
        resource by means of a string or number conforming to a formal
        identification system. Example formal identification systems
        include the Uniform Resource Identifier (URI) (including the
        Uniform Resource Locator (URL)), the Digital Object Identifier
        (DOI) and the International Standard Book Number (ISBN).
        </ns1:div>
    :cvar DC_FORMAT: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">The physical or
        digital manifestation of the resource.</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Typically, Format may include the media-type
        or dimensions of the resource. Format may be used to determine
        the software, hardware or other equipment needed to display or
        operate the resource. Examples of dimensions include size and
        duration. Recommended best practice is to select a value from a
        controlled vocabulary (for example, the list of Internet Media
        Types [MIME] defining computer media formats). </ns1:div>
    :cvar DC_RELATION: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">A reference to a
        related resource.</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description">Recommended best
        practice is to reference the resource by means of a string or
        number conforming to a formal identification system. </ns1:div>
    :cvar DC_RIGHTS: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">Information about
        rights held in and over the resource.</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Typically, a Rights element will contain a
        rights management statement for the resource, or reference a
        service providing such information. Rights information often
        encompasses Intellectual Property Rights (IPR), Copyright, and
        various Property Rights. If the Rights element is absent, no
        assumptions can be made about the status of these and other
        rights with respect to the resource. </ns1:div>
    :cvar DC_SUBJECT: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">The topic of the
        content of the resource.</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Typically, a Subject will be expressed as
        keywords, key phrases or classification codes that describe a
        topic of the resource. Recommended best practice is to select a
        value from a controlled vocabulary or formal classification
        scheme. </ns1:div>
    :cvar DC_TITLE: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">A name given to the
        resource.</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description">Typically, a Title
        will be a name by which the resource is formally known.
        </ns1:div>
    :cvar DC_TYPE: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">The nature or genre
        of the content of the resource.</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Type includes terms describing general
        categories, functions, genres, or aggregation levels for
        content. Recommended best practice is to select a value from a
        controlled vocabulary (for example, the working draft list of
        Dublin Core Types [DCT1]). To describe the physical or digital
        manifestation of the resource, use the FORMAT element.
        </ns1:div>
    :cvar DC_CONTRIBUTOR: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">An entity
        responsible for making contributions to the content of the
        resource.</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description">Examples of a
        Contributor include a person, an organisation, or a service.
        Typically, the name of a Contributor should be used to indicate
        the entity. </ns1:div>
    :cvar DC_CREATOR: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">An entity primarily
        responsible for making the content of the resource.</ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Examples of a Creator include a person, an
        organisation, or a service. Typically, the name of a Creator
        should be used to indicate the entity. </ns1:div>
    :cvar DC_PUBLISHER: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">An entity
        responsible for making the resource available</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Examples of a Publisher include a person, an
        organisation, or a service. Typically, the name of a Publisher
        should be used to indicate the entity. </ns1:div>
    :cvar DC_SOURCE: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">A Reference to a
        resource from which the present resource is derived.</ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">The present resource may be derived from the
        Source resource in whole or in part. Recommended best practice
        is to reference the resource by means of a string or number
        conforming to a formal identification system. </ns1:div>
    :cvar DC_LANGUAGE: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">A language of the
        intellectual content of the resource.</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Recommended best practice for the values of
        the Language element is defined by RFC 1766 [RFC1766] which
        includes a two-letter Language Code (taken from the ISO 639
        standard [ISO639]), followed optionally, by a two-letter Country
        Code (taken from the ISO 3166 standard [ISO3166]). For example,
        'en' for English, 'fr' for French, or 'en-uk' for English used
        in the United Kingdom. </ns1:div>
    :cvar DC_DATE: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">A date associated
        with an event in the life cycle of the resource.</ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Typically, Date will be associated with the
        creation or availability of the resource. Recommended best
        practice for encoding the date value is defined in a profile of
        ISO 8601 [W3CDTF] and follows the YYYY-MM-DD format. </ns1:div>
    :cvar CMLM_SAFETY: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">Entry contains
        information relating to chemical safety</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Typically the content will be a reference to
        a handbook, MSDS, threshhold or other human-readable string
        </ns1:div>
    :cvar CMLM_INSILICO: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">Part or whole of
        the information was computer-generated</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description">Typically the content will be the name of a
        method or a program </ns1:div>
    :cvar CMLM_STRUCTURE: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="definition">3D structure
        included</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description">details included
        </ns1:div>
    :cvar CMLM_REACTION:
    :cvar CMLM_IDENTIFIER:
    :cvar OTHER:
    """
    DC_COVERAGE = "dc:coverage"
    DC_DESCRIPTION = "dc:description"
    DC_IDENTIFIER = "dc:identifier"
    DC_FORMAT = "dc:format"
    DC_RELATION = "dc:relation"
    DC_RIGHTS = "dc:rights"
    DC_SUBJECT = "dc:subject"
    DC_TITLE = "dc:title"
    DC_TYPE = "dc:type"
    DC_CONTRIBUTOR = "dc:contributor"
    DC_CREATOR = "dc:creator"
    DC_PUBLISHER = "dc:publisher"
    DC_SOURCE = "dc:source"
    DC_LANGUAGE = "dc:language"
    DC_DATE = "dc:date"
    CMLM_SAFETY = "cmlm:safety"
    CMLM_INSILICO = "cmlm:insilico"
    CMLM_STRUCTURE = "cmlm:structure"
    CMLM_REACTION = "cmlm:reaction"
    CMLM_IDENTIFIER = "cmlm:identifier"
    OTHER = "other"


@dataclass
class Object:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">An object which might occur in scientific data or
    narrative</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>Deliberately vague. Thus an instrument might be built from sub component
    objects, or a program could be composed of smaller modules (objects).
    Unrestricted content model</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>&lt;object title="frog" type="amphibian" count="5"&gt;
    &lt;scalar dataType="xsd:float" title="length" units="unit:cm"&gt;5&lt;/scalar&gt;
    &lt;obj1/&gt;
    &lt;/object&gt;
    </ns1:pre>
    </ns1:div>

    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar type: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">Type of the object.
        Uncontrolled semantics</ns1:div>
    :ivar count:
    :ivar content:
    """
    class Meta:
        name = "object"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    type: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    count: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "min_inclusive": 1,
            "max_inclusive": 999999999999,
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        }
    )


@dataclass
class Observation:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">An observation or occurrence</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>A container for any events that
    need to be recorded, whether planned or not. They can include notes,
    measurements, conditions that may be referenced elsewhere, etc. There are
    no controlled semantics </ns1:p>
    <ns1:div class="example">
    <ns1:pre>&lt;observation type="ornithology"&gt;
    &lt;object title="sparrow" count="3"/&gt;
    &lt;observ/&gt;
    &lt;/observation&gt;
    </ns1:pre>
    </ns1:div>
    </ns1:div>

    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar type: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">Type of observation
        (uncontrolled vocabulary)</ns1:div>
    :ivar count:
    :ivar content:
    """
    class Meta:
        name = "observation"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    type: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    count: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "min_inclusive": 1,
            "max_inclusive": 999999999999,
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        }
    )


class RelatedEntryValue(Enum):
    PARENT = "parent"
    PARTITIVE_PARENT = "partitiveParent"
    CHILD = "child"
    PARTITIVE_CHILD = "partitiveChild"
    RELATED = "related"
    SYNONYM = "synonym"
    QUASI_SYNONYM = "quasi-synonym"
    ANTONYM = "antonym"
    HOMONYM = "homonym"
    SEE = "see"
    SEE_ALSO = "seeAlso"
    ABBREVIATION = "abbreviation"
    ACRONYM = "acronym"


@dataclass
class Stmml:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">An element to hold stmml data.</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p><ns1:tt>stmml</ns1:tt> holds stmml data under a single
    generic container. Other namespaces may be present as children.
    No semantics implied.
    </ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;stmml&gt;
    &lt;actionList&gt;
    &lt;action&gt;&lt;/action&gt;
    &lt;/actionList&gt;
    &lt;object&gt;&lt;/object&gt;
    &lt;observation&gt;&lt;/observation&gt;
    &lt;!-- ==================== DICTIONARY =========== --&gt;
    &lt;dictionary&gt;
    &lt;annotation&gt;
    &lt;documentation&gt;&lt;/documentation&gt;
    &lt;appinfo&gt;&lt;/appinfo&gt;
    &lt;/annotation&gt;
    &lt;entry term="foo"&gt;
    &lt;definition&gt;&lt;/definition&gt;
    &lt;alternative&gt;&lt;/alternative&gt;
    &lt;description&gt;&lt;/description&gt;
    &lt;enumeration&gt;&lt;/enumeration&gt;
    &lt;relatedEntry&gt;&lt;/relatedEntry&gt;
    &lt;/entry&gt;
    &lt;/dictionary&gt;
    &lt;!-- =================  METADATA ================== --&gt;
    &lt;metadataList&gt;
    &lt;metadata&gt;&lt;/metadata&gt;
    &lt;/metadataList&gt;
    &lt;!-- =================  SCIENTIFIC UNITS ================== --&gt;
    &lt;unitList&gt;
    &lt;unitType id="ut1" name="u"&gt;
    &lt;dimension name="mass"&gt;&lt;/dimension&gt;
    &lt;/unitType&gt;
    &lt;unit id="u1"&gt;&lt;/unit&gt;
    &lt;/unitList&gt;
    &lt;/stmml&gt;
    </ns1:pre>
    </ns1:div>

    :ivar any_element:
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    """
    class Meta:
        name = "stmml"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    any_element: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )


@dataclass
class ActionList:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A container for a group of <ns1:a
    href="el.action">actions</ns1:a></ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p><ns1:tt>ActionList</ns1:tt> contains a series of <ns1:tt>action</ns1:tt>s or
    nested <ns1:tt>actionList</ns1:tt>s.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">See examples in <ns1:a href="el.action">action</ns1:a></ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;!-- demonstrating parallel and sequential actions --&gt;
    &lt;actionList order="parallel" endCondition="all food cooked"&gt;
    &lt;!-- meat and potatoes are cooked in parallel --&gt;
    &lt;actionList title="meat"&gt;
    &lt;action title="cook" endCondition="cooked"&gt;
    &lt;p&gt;Roast meat&lt;/p&gt;
    &lt;/action&gt;
    &lt;action&gt;&lt;p&gt;Keep warm in oven&lt;/p&gt;&lt;/action&gt;
    &lt;/actionList&gt;
    &lt;actionList title="vegetables"&gt;
    &lt;actionList title="cookVeg" endCondition="cooked"&gt;
    &lt;action title="boil water" endCondition="water boiling"&gt;
    &lt;p&gt;Heat water&lt;/p&gt;
    &lt;/action&gt;
    &lt;action title="cook" endCondition="potatoes cooked"&gt;
    &lt;p&gt;Cook potatoes&lt;/p&gt;
    &lt;/action&gt;
    &lt;/actionList&gt;
    &lt;action&gt;&lt;p&gt;Keep warm in oven&lt;/p&gt;&lt;/action&gt;
    &lt;/actionList&gt;
    &lt;/actionList&gt;
    </ns1:pre>
    </ns1:div>

    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar start: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The start
        time</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>The start
        time in any allowable XSD representation of date, time or
        dateTime. This will normally be a clock time or date.</ns1:p>
        </ns1:div>
    :ivar start_condition:
    :ivar duration:
    :ivar end: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The end time</ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>The start time in any allowable XSD
        representation of date, time or dateTime. This will normally be
        a clock time or date.</ns1:p> </ns1:div>
    :ivar end_condition: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>The end
        condition</ns1:p> </ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>At present
        a human-readable string describing some condition when the ac
        tion should end. As XML develops it may be possible to add
        machine-processable semantics in this field.</ns1:p> </ns1:div>
    :ivar units: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">Units for the time
        attributes</ns1:div>
    :ivar count: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">Number of times the
        action should be repeated</ns1:div>
    :ivar type: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The type of the
        actionList; no defined semantics</ns1:div>
    :ivar order: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description">Describes whether
        child elements are sequential or parallel</ns1:div>
    :ivar content:
    """
    class Meta:
        name = "actionList"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    start: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    start_condition: Optional[str] = field(
        default=None,
        metadata={
            "name": "startCondition",
            "type": "Attribute",
        }
    )
    duration: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    end: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    end_condition: Optional[str] = field(
        default=None,
        metadata={
            "name": "endCondition",
            "type": "Attribute",
        }
    )
    units: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    count: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "min_inclusive": 1,
            "max_inclusive": 999999999999,
        }
    )
    type: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    order: ActionListOrder = field(
        default=ActionListOrder.SEQUENTIAL,
        metadata={
            "type": "Attribute",
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        }
    )


@dataclass
class Alternative:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">

    <ns1:p>An alternative name for an entry</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>At present a child of <ns1:a href="#el.entry">entry</ns1:a> which represents
    an alternative string that refers to the concept. There is a partial controlled
    vocabulary in <ns1:tt>alternativeType</ns1:tt> with values such as :
    </ns1:p>
    <ns1:ul>
    <ns1:li>synonym</ns1:li>
    <ns1:li>acronym</ns1:li>
    <ns1:li>abbreviation</ns1:li>
    </ns1:ul>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;entry term="ammonia" id="a1"&gt;
    &lt;alternative type="synonym"&gt;Spirits of hartshorn&lt;/alternative&gt;
    &lt;alternative type="my:formula"&gt;NH3&lt;/alternative&gt;
    &lt;/entry&gt;
    </ns1:pre>
    </ns1:div>
    """
    class Meta:
        name = "alternative"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    value: str = field(
        default="",
        metadata={
            "required": True,
        }
    )
    type: Optional[AlternativeValue] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )


@dataclass
class Annotation:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">

    <ns1:p>A documentation container similar to <ns1:tt>annotation</ns1:tt> in XML Schema.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>A documentation container similar to <ns1:tt>annotation</ns1:tt> in XML Schema.
    At present this is experimental and designed to be used for dictionaries, units, etc.
    One approach is to convert these into XML Schemas when the <ns1:tt>documentation</ns1:tt>
    and <ns1:tt>appinfo</ns1:tt> children will emerge in their correct position in the
    derived schema.</ns1:p>
    <ns1:p>It is possible that this may develop as a useful tool for annotating components
    of complex objects such as molecules. </ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;entry term="matrix"&gt;
    &lt;annotation&gt;
    &lt;documentation&gt;This refers to mathematical matrices&lt;/documentation&gt;
    &lt;appinfo&gt;... some code to describe and support matrices ...&lt;/appinfo&gt;
    &lt;/annotation&gt;
    &lt;/entry&gt;
    </ns1:pre>
    </ns1:div>
    """
    class Meta:
        name = "annotation"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    source: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
            "choices": (
                {
                    "name": "documentation",
                    "type": Documentation,
                },
                {
                    "name": "appinfo",
                    "type": Appinfo,
                },
            ),
        }
    )


@dataclass
class Array:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary"> A homogenous 1-dimensional array of similar
    objects.</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p><ns1:tt>array</ns1:tt> manages a homogenous 1-dimensional array of similar objects. These
    can be encoded as strings (i.e. XSD-like datatypes) and are concatenated as
    string content. The size of the array should always be &gt;= 1.
    </ns1:p>
    <ns1:p>The default delimiter is whitespace. The <ns1:tt>normalize-space()</ns1:tt> function of
    XSLT could be used to normalize all whitespace to single spaces and this would not affect
    the value of the array elements. To extract the elements <ns1:tt>java.lang.StringTokenizer</ns1:tt>
    could be used. If the elements themselves contain whitespace then a different delimiter
    must be used and is identified through the <ns1:tt>delimiter</ns1:tt> attribute. This method is
    mandatory if it is required to represent empty strings. If a delimiter is used it MUST
    start and end the array - leading and trailing whitespace is ignored. Thus <ns1:tt>size+1</ns1:tt>
    occurrences of the delimiter character are required. If non-normalized whitespace is to be
    encoded (e.g. newlines, tabs, etc) you are recommended to translate it character-wise
    to XML character entities.
    </ns1:p>
    <ns1:p>Note that normal Schema validation tools cannot validate the elements
    of <ns1:b>array</ns1:b> (they are defined as <ns1:tt>string</ns1:tt>) However if the string is
    split, a temporary schema
    can be constructed from the type and used for validation. Also the type
    can be contained in a dictionary and software could decide to retrieve this
    and use it for validation.</ns1:p>
    <ns1:p>When the elements of the <ns1:tt>array</ns1:tt> are not simple scalars
    (e.g. <ns1:a href="el.scalar">scalar</ns1:a>s with a value and an error, the
    <ns1:tt>scalar</ns1:tt>s should be used as the elements. Although this is
    verbose, it is simple to understand. If there is a demand for
    more compact representations, it will be possible to define the
    syntax in a later version.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;array size="5" title="value"
    dataType="xsd:decimal"&gt;  1.23 2.34 3.45 4.56 5.67&lt;/array&gt;
    </ns1:pre>
    <ns1:p>the <ns1:tt>size</ns1:tt> attribute is not mandatory but provides a useful validity
    check): </ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;array size="5" title="initials" dataType="xsd:string"
    delimiter="/"&gt;/A B//C/D-E/F/&lt;/array&gt;
    </ns1:pre>
    <ns1:p>Note that the second array-element is the empty string ''.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;array title="mass" size="4"
    units="unit:g"
    errorBasis="observedStandardDeviation"
    minValues="10 11 10 9"
    maxValues="12 14 12 11"
    errorValues="1 2 1 1"
    dataType="xsd:float"&gt;11 12.5 10.9 10.2
    &lt;/array&gt;
    </ns1:pre>
    </ns1:div>

    :ivar value:
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar data_type: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">all elements of the
        array must have the same dataType</ns1:div>
    :ivar error_values: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">an optional array of
        error values for numeric arrays</ns1:div>
    :ivar error_basis: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A string describing
        the basis of the errors</ns1:div>
    :ivar min_values: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">an optional array of
        minimum values for numeric arrays</ns1:div>
    :ivar max_values: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">an optional array of
        maximum values for numeric arrays</ns1:div>
    :ivar units: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">a string denoting the
        units (recommended for numeric quantities!!). All elements must
        have the same units</ns1:div>
    :ivar delimiter:
    :ivar size:
    """
    class Meta:
        name = "array"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    value: str = field(
        default="",
        metadata={
            "required": True,
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    data_type: DataTypeType = field(
        default=DataTypeType.XSD_STRING,
        metadata={
            "name": "dataType",
            "type": "Attribute",
        }
    )
    error_values: List[Decimal] = field(
        default_factory=list,
        metadata={
            "name": "errorValues",
            "type": "Attribute",
            "tokens": True,
        }
    )
    error_basis: Optional[ErrorBasisType] = field(
        default=None,
        metadata={
            "name": "errorBasis",
            "type": "Attribute",
        }
    )
    min_values: List[Decimal] = field(
        default_factory=list,
        metadata={
            "name": "minValues",
            "type": "Attribute",
            "tokens": True,
        }
    )
    max_values: List[Decimal] = field(
        default_factory=list,
        metadata={
            "name": "maxValues",
            "type": "Attribute",
            "tokens": True,
        }
    )
    units: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    delimiter: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    size: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )


@dataclass
class Dimension:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A dimension supporting scientific units</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>This will be primarily used within the defintion of
    <ns1:a href="el.unit">units</ns1:a>s.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;unitType id="energy" name="energy"&gt;
    &lt;dimension name="length"/&gt;
    &lt;dimension name="mass"/&gt;
    &lt;dimension name="time" power="-1"/&gt;
    &lt;/unitType&gt;
    </ns1:pre>
    </ns1:div>

    :ivar name: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The type of the
        dimension</ns1:div>
    :ivar power: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The power to which the
        dimension should be raised</ns1:div>
    """
    class Meta:
        name = "dimension"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    name: Optional[DimensionType] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    power: Decimal = field(
        default=Decimal("1"),
        metadata={
            "type": "Attribute",
        }
    )


@dataclass
class Link:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">An internal or external link to STMML or other
    object(s)</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p><ns1:tt>link</ns1:tt> is an internal or external link to STMML or other object(s). </ns1:p>
    <ns1:p><ns1:b>Semantics are similar to XLink, but simpler and only a subset is implemented.</ns1:b>
    This is intended to make the instances easy to create and read, and software
    relatively easy to implement. The architecture is:</ns1:p>
    <ns1:ul>
    <ns1:li><ns1:b>A single element (<ns1:tt>link</ns1:tt>) used for all linking purposes</ns1:b>. </ns1:li>
    <ns1:li><ns1:b>The link types are determined by the <ns1:tt>type</ns1:tt> attribute and can be:</ns1:b>.
    <ns1:ul><ns1:li><ns1:b>locator</ns1:b>. This points to a single target and must carry either a <ns1:tt>ref</ns1:tt> or <ns1:tt>href</ns1:tt> attribute.
    <ns1:tt>locator</ns1:tt> links are usually children of an extended link.
    <ns1:li><ns1:b>arc</ns1:b>. This is a 1:1 link with both ends (<ns1:tt>from</ns1:tt> and <ns1:tt>to</ns1:tt>) defined.</ns1:li><ns1:li><ns1:b>extended</ns1:b>. This is usually a parent of several locator links and serves
    to create a grouping of link ends (i.e. a list of references in documents).</ns1:li>
    Many-many links can be built up from arcs linking extended elements</ns1:li></ns1:ul><ns1:p>All links can have optional <ns1:tt>role</ns1:tt> attributes. The semantics of this are not defined;
    you are encouraged to use a URI as described in the XLink specification.</ns1:p><ns1:p>There are two address spaces: </ns1:p><ns1:ul><ns1:li>The <ns1:tt>href</ns1:tt> attribute on locators behaves in the same way as <ns1:tt>href</ns1:tt> in
    HTML and is of type <ns1:tt>xsd:anyURI</ns1:tt>. Its primary use is to use XPointer to reference
    elements outside the document.</ns1:li><ns1:li>The <ns1:tt>ref</ns1:tt> attribute on locators and the <ns1:tt>from</ns1:tt> and <ns1:tt>to</ns1:tt>
    attributes on <ns1:tt>arc</ns1:tt>s refer to IDs (<ns1:em>without</ns1:em> the '#' syntax).</ns1:li></ns1:ul><ns1:p>Note: several other specific linking mechanisms are defined elsewhere in STM. <ns1:a href="         el.relatedEntry">relatedEntry</ns1:a> should be used in dictionaries, and <ns1:a href="st.dictRef">dictRef</ns1:a>
    should be used to link to dictionaries. There are no required uses of <ns1:tt>link</ns1:tt> in STM-ML
    but we have used it to map atoms, electrons and bonds in reactions in CML</ns1:p></ns1:li>
    </ns1:ul>
    <ns1:p><ns1:b>Relation to XLink</ns1:b>.
    At present (2002) we are not aware of generic XLink
    processors from which we would benefit, so the complete implementation brings little
    extra value.
    Among the simplifications from Xlink are:</ns1:p>
    <ns1:ul>
    <ns1:li><ns1:tt>type</ns1:tt> supports only <ns1:tt>extended</ns1:tt>, <ns1:tt>locator</ns1:tt> and <ns1:tt>arc</ns1:tt></ns1:li>
    <ns1:li><ns1:tt>label</ns1:tt> is not supported and <ns1:tt>id</ns1:tt>s are used as targets of links.</ns1:li>
    <ns1:li><ns1:tt>show</ns1:tt> and <ns1:tt>actuate</ns1:tt> are not supported.</ns1:li>
    <ns1:li><ns1:tt>xlink:title</ns1:tt> is not supported (all STM elements can have a <ns1:tt>title</ns1:tt>
    attribute).</ns1:li>
    <ns1:li><ns1:tt>xlink:role</ns1:tt> supports any string (i.e. does not have to be a namespaced resource).
    This mechanism can, of course, still be used and we shall promote it where STM
    benefits from it</ns1:li>
    <ns1:li>The <ns1:tt>to</ns1:tt> and <ns1:tt>from</ns1:tt> attributes point to IDs rather than labels</ns1:li>
    <ns1:li>The xlink namespace is not used</ns1:li>
    <ns1:li>It is not intended to create independent linkbases, although some collections of
    links may have this property and stand outside the documents they link to</ns1:li>
    </ns1:ul>
    </ns1:div>

    :ivar any_element:
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar from_value: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The starting point of
        an arc</ns1:div>
    :ivar to: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The endpoint of an
        arc</ns1:div>
    :ivar ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An ID referenced
        within a locator</ns1:div>
    :ivar role: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The role of the link.
        Xlink adds semantics through a URI; we shall not be this strict.
        We shall not normally use this mechanism and use dictionaries
        instead</ns1:div>
    :ivar href: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The target of the
        (locator) link, outside the document</ns1:div>
    :ivar type: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The type of the
        link</ns1:div>
    """
    class Meta:
        name = "link"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    any_element: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    from_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "from",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    to: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    ref: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    role: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    href: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type: Optional[LinkType] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )


@dataclass
class Matrix:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A rectangular matrix of any quantities</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>By default <ns1:tt>matrix</ns1:tt> represents
    a rectangular matrix of any quantities
    representable as XSD or STMML dataTypes. It consists of
    <ns1:tt>rows*columns</ns1:tt> elements, where <ns1:tt>columns</ns1:tt> is the
    fasting moving index. Assuming the elements are counted from 1 they are
    ordered <ns1:tt>V[1,1],V[1,2],...V[1,columns],V[2,1],V[2,2],...V[2,columns],
    ...V[rows,1],V[rows,2],...V[rows,columns]</ns1:tt></ns1:p>
    <ns1:p>By default whitespace is used to separate matrix elements; see
    <ns1:a href="el.array">array</ns1:a> for details. There are NO characters or markup
    delimiting the end of rows; authors must be careful!. The <ns1:tt>columns</ns1:tt>
    and <ns1:tt>rows</ns1:tt> attributes have no default values; a row vector requires
    a <ns1:tt>rows</ns1:tt> attribute of 1.</ns1:p>
    <ns1:p><ns1:tt>matrix</ns1:tt> also supports many types of square matrix, but at present we
    require all elements to be given, even if the matrix is symmetric, antisymmetric
    or banded diagonal. The <ns1:tt>matrixType</ns1:tt> attribute allows software to
    validate and process the type of matrix.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;matrix id="m1" title="mattrix-1" dictRef="foo:bar"
    rows="3" columns="3" dataType="xsd:decimal"
    delimiter="|" matrixType="squareSymmetric" units="unit:m"
    &gt;|1.1|1.2|1.3|1.2|2.2|2.3|1.3|2.3|3.3!&lt;/matrix&gt;
    </ns1:pre>
    </ns1:div>

    :ivar value:
    :ivar data_type:
    :ivar delimiter:
    :ivar rows: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>Number of
        rows</ns1:p> </ns1:div>
    :ivar columns: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>Number of
        columns</ns1:p> </ns1:div>
    :ivar units: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>units
        (recommended for numeric quantities!!)</ns1:p> </ns1:div>
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar matrix_type: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>Type of matrix
        (mainly square ones)</ns1:p> </ns1:div>
    :ivar error_values: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">an optional array of
        error values for numeric matrices</ns1:div>
    :ivar error_basis: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A string describing
        the basis of the errors</ns1:div>
    :ivar min_values: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">an optional array of
        minimum values for numeric matrices</ns1:div>
    :ivar max_values: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">an optional array of
        maximum values for numeric matrices</ns1:div>
    """
    class Meta:
        name = "matrix"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    value: str = field(
        default="",
        metadata={
            "required": True,
        }
    )
    data_type: DataTypeType = field(
        default=DataTypeType.XSD_FLOAT,
        metadata={
            "name": "dataType",
            "type": "Attribute",
        }
    )
    delimiter: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    rows: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    columns: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    units: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    matrix_type: Optional[Union[str, MatrixTypeValue]] = field(
        default=None,
        metadata={
            "name": "matrixType",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    error_values: List[Decimal] = field(
        default_factory=list,
        metadata={
            "name": "errorValues",
            "type": "Attribute",
            "tokens": True,
        }
    )
    error_basis: Optional[ErrorBasisType] = field(
        default=None,
        metadata={
            "name": "errorBasis",
            "type": "Attribute",
        }
    )
    min_values: List[Decimal] = field(
        default_factory=list,
        metadata={
            "name": "minValues",
            "type": "Attribute",
            "tokens": True,
        }
    )
    max_values: List[Decimal] = field(
        default_factory=list,
        metadata={
            "name": "maxValues",
            "type": "Attribute",
            "tokens": True,
        }
    )


@dataclass
class Metadata:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A general container for metadata</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>A general container for metadata, including at least
    Dublin Core (DC) and CML-specific metadata</ns1:p>
    <ns1:p>In its simple form each element provides a name and content in a similar
    fashion to the <ns1:tt>meta</ns1:tt> element in HTML. <ns1:tt>metadata</ns1:tt> may have simpleContent
    (i.e. a string for adding further information - this is not controlled).</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;list&gt;
    &lt;metadataList&gt;
    &lt;metadata name="dc:coverage" content="Europe"/&gt;
    &lt;metadata name="dc:description" content="Ornithological chemistry"/&gt;
    &lt;metadata name="dc:identifier"  content="ISBN:1234-5678"/&gt;
    &lt;metadata name="dc:format" content="printed"/&gt;
    &lt;metadata name="dc:relation" content="abc:def123"/&gt;
    &lt;metadata name="dc:rights" content="licence:GPL"/&gt;
    &lt;metadata name="dc:subject" content="Informatics"/&gt;
    &lt;metadata name="dc:title" content="birds"/&gt;
    &lt;metadata name="dc:type" content="bird books on chemistry"/&gt;
    &lt;metadata name="dc:contributor" content="Tux Penguin"/&gt;
    &lt;metadata name="dc:creator" content="author"/&gt;
    &lt;metadata name="dc:publisher" content="Penguinone publishing"/&gt;
    &lt;metadata name="dc:source" content="penguinPub"/&gt;
    &lt;metadata name="dc:language" content="en-GB"/&gt;
    &lt;metadata name="dc:date" content="1752-09-10"/&gt;
    &lt;/metadataList&gt;
    &lt;metadataList&gt;
    &lt;metadata name="cmlm:safety" content="mostly harmless"/&gt;
    &lt;metadata name="cmlm:insilico" content="electronically produced"/&gt;
    &lt;metadata name="cmlm:structure" content="penguinone"/&gt;
    &lt;metadata name="cmlm:reaction" content="synthesis of penguinone"/&gt;
    &lt;metadata name="cmlm:identifier" content="smiles:O=C1C=C(C)C(C)(C)C(C)=C1"/&gt;
    &lt;/metadataList&gt;
    &lt;/list&gt;
    </ns1:pre>
    </ns1:div>

    :ivar value:
    :ivar name: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The metadata
        type</ns1:div>
    :ivar content: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The metadata</ns1:div>
    """
    class Meta:
        name = "metadata"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    value: str = field(
        default="",
        metadata={
            "required": True,
        }
    )
    name: Optional[MetadataType] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    content: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )


@dataclass
class RelatedEntry:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">An entry related in some way to a dictionary entry,
    scientific units, etc.</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>The range of relationships is not restricted
    but should include parents, aggregation, seeAlso
    etc. dataCategories from ISO12620 can be referenced through the <ns1:tt>namespaced</ns1:tt>
    mechanism.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;stm:entry id="a14" term="Autoreceptor"
    xmlns:stm="http://www.xml-cml.org/schema/core"&gt;
    &lt;stm:definition&gt;An &lt;strong&gt;autoreceptor&lt;/strong&gt;, present at a nerve ending, is
    a &lt;a href="#r1"&gt;receptor&lt;/a&gt;
    that regulates, via positive or negative feedback processes, the
    synthesis and/or release of its own physiological ligand.
    &lt;/stm:definition&gt;
    &lt;stm:relatedEntry type="seeAlso" href="#h4"&gt;Heteroreceptor).&lt;/stm:relatedEntry&gt;
    &lt;stm:relatedEntry type="my:antonym" href="#h4"&gt;antiheteroreceptor).&lt;/stm:relatedEntry&gt;
    &lt;relatedEntry1/&gt;
    &lt;/stm:entry&gt;
    </ns1:pre>
    </ns1:div>
    """
    class Meta:
        name = "relatedEntry"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    type: Optional[RelatedEntryValue] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    href: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        }
    )


@dataclass
class Scalar:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">An element to hold scalar data.</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p><ns1:tt>scalar</ns1:tt> holds scalar data under a single
    generic container. The semantics are usually resolved by
    linking to a dictionary.
    <ns1:b>scalar</ns1:b> defaults to a scalar string but
    has attributes which affect the type.
    </ns1:p>
    <ns1:p><ns1:tt>scalar</ns1:tt> does not necessarily reflect a physical object (for which
    <ns1:a href="el.object">object</ns1:a> should be used). It may reflect a property of an object
    such as temperature, size, etc. </ns1:p>
    <ns1:p>Note that normal Schema validation tools cannot validate the data type
    of <ns1:b>scalar</ns1:b> (it is defined as <ns1:tt>string</ns1:tt>), but that a temporary schema
    can be constructed from the type and used for validation. Also the type
    can be contained in a dictionary and software could decide to retrieve this
    and use it for validation.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;scalar
    dataType="xsd:decimal"
    errorValue="1.0"
    errorBasis="observedStandardDeviation"
    title="body weight"
    dictRef="zoo:bodywt"
    units="units:g"&gt;34.3&lt;/scalar&gt;
    </ns1:pre>
    </ns1:div>

    :ivar value:
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar data_type:
    :ivar error_value:
    :ivar error_basis:
    :ivar min_value:
    :ivar max_value:
    :ivar units:
    """
    class Meta:
        name = "scalar"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    value: str = field(
        default="",
        metadata={
            "required": True,
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    data_type: DataTypeType = field(
        default=DataTypeType.XSD_STRING,
        metadata={
            "name": "dataType",
            "type": "Attribute",
        }
    )
    error_value: Optional[Decimal] = field(
        default=None,
        metadata={
            "name": "errorValue",
            "type": "Attribute",
        }
    )
    error_basis: Optional[ErrorBasisType] = field(
        default=None,
        metadata={
            "name": "errorBasis",
            "type": "Attribute",
        }
    )
    min_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "minValue",
            "type": "Attribute",
        }
    )
    max_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "maxValue",
            "type": "Attribute",
        }
    )
    units: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )


@dataclass
class Enumeration:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">An enumeration of string values associated with an <ns1:a
    href="el.entry">entry</ns1:a></ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>An enumeration of string values. Used where a dictionary entry constrains
    the possible values in a document instance. The dataTypes (if any) must all be
    identical and are defined by the dataType of the containing element.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;entry term="crystal system" id="cryst1" dataType="string"&gt;
    &lt;definition&gt;A crystal system&lt;/definition&gt;
    &lt;enumeration value="triclinic"&gt;
    &lt;annotation&gt;
    &lt;documentation&gt;
    &lt;div class="summary"&gt;No constraints on lengths and angles&lt;/div&gt;
    &lt;/documentation&gt;
    &lt;/annotation&gt;
    &lt;/enumeration&gt;
    &lt;enumeration value="monoclinic"&gt;
    &lt;annotation&gt;
    &lt;documentation&gt;
    &lt;div class="summary"&gt;Two cell angles are right angles; no other constraints&lt;/div&gt;
    &lt;/documentation&gt;
    &lt;/annotation&gt;
    &lt;/enumeration&gt;
    &lt;enumeration value="orthorhombic"&gt;
    &lt;annotation&gt;
    &lt;documentation&gt;
    &lt;div class="summary"&gt;All three angles are right angles; no other constraints&lt;/div&gt;
    &lt;/documentation&gt;
    &lt;/annotation&gt;
    &lt;/enumeration&gt;
    &lt;enumeration value="tetragonal"&gt;
    &lt;annotation&gt;
    &lt;documentation&gt;
    &lt;div class="summary"&gt;Fourfold axis of symmetry; All three angles are right angles; two equal cell lengths; no other constraints&lt;/div&gt;
    &lt;/documentation&gt;
    &lt;/annotation&gt;
    &lt;/enumeration&gt;
    &lt;enumeration value="trigonal"&gt;
    &lt;annotation&gt;
    &lt;documentation&gt;
    &lt;div class="summary"&gt;Threefold axis of symmetry; Two angles are right angles; one is 120 degrees; two equal lengths; no other constraints&lt;/div&gt;
    &lt;/documentation&gt;
    &lt;/annotation&gt;
    &lt;/enumeration&gt;
    &lt;enumeration value="hexagonal"&gt;
    &lt;annotation&gt;
    &lt;documentation&gt;
    &lt;div class="summary"&gt;Sixfold axis of symmetry; Two angles are right angles; one is 120 degrees; two equal lengths; no other constraints&lt;/div&gt;
    &lt;/documentation&gt;
    &lt;/annotation&gt;
    &lt;/enumeration&gt;
    &lt;enumeration value="cubic"&gt;
    &lt;annotation&gt;
    &lt;documentation&gt;
    &lt;div class="summary"&gt;All three angles are right angles; all cell lengths are equal&lt;/div&gt;
    &lt;/documentation&gt;
    &lt;/annotation&gt;
    &lt;/enumeration&gt;
    &lt;/entry&gt;
    </ns1:pre>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>An enumeration of string values. The dataTypes (if any) must all be
    identical and are defined by the dataType of the containing element.</ns1:p>
    <ns1:p>Documentation can be added through an <ns1:a href="el.enumeration">enumeration</ns1:a>
    child</ns1:p>
    </ns1:div>

    :ivar annotation:
    :ivar value: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">The value of the
        enumerated element.</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> Must be compatible with the dataType of the
        containing element (not schema-checkable directly but possible
        if dictionary is transformed to schema).</ns1:div>
    """
    class Meta:
        name = "enumeration"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    annotation: Optional[Annotation] = field(
        default=None,
        metadata={
            "type": "Element",
        }
    )
    value: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )


@dataclass
class MetadataList:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A general container for metadata elements</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>&lt;list&gt;
    &lt;metadataList&gt;
    &lt;metadata name="dc:coverage" content="Europe"/&gt;
    &lt;metadata name="dc:description" content="Ornithological chemistry"/&gt;
    &lt;metadata name="dc:identifier"  content="ISBN:1234-5678"/&gt;
    &lt;metadata name="dc:format" content="printed"/&gt;
    &lt;metadata name="dc:relation" content="abc:def123"/&gt;
    &lt;metadata name="dc:rights" content="licence:GPL"/&gt;
    &lt;metadata name="dc:subject" content="Informatics"/&gt;
    &lt;metadata name="dc:title" content="birds"/&gt;
    &lt;metadata name="dc:type" content="bird books on chemistry"/&gt;
    &lt;metadata name="dc:contributor" content="Tux Penguin"/&gt;
    &lt;metadata name="dc:creator" content="author"/&gt;
    &lt;metadata name="dc:publisher" content="Penguinone publishing"/&gt;
    &lt;metadata name="dc:source" content="penguinPub"/&gt;
    &lt;metadata name="dc:language" content="en-GB"/&gt;
    &lt;metadata name="dc:date" content="1752-09-10"/&gt;
    &lt;/metadataList&gt;
    &lt;metadataList&gt;
    &lt;metadata name="cmlm:safety" content="mostly harmless"/&gt;
    &lt;metadata name="cmlm:insilico" content="electronically produced"/&gt;
    &lt;metadata name="cmlm:structure" content="penguinone"/&gt;
    &lt;metadata name="cmlm:reaction" content="synthesis of penguinone"/&gt;
    &lt;metadata name="cmlm:identifier" content="smiles:O=C1C=C(C)C(C)(C)C(C)=C1"/&gt;
    &lt;/metadataList&gt;
    &lt;/list&gt;
    </ns1:pre>
    </ns1:div>
    """
    class Meta:
        name = "metadataList"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    metadata: List[Metadata] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "min_occurs": 1,
        }
    )


@dataclass
class Table:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A rectangular table of any quantities</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>By default <ns1:tt>table</ns1:tt> represents a rectangular table of any quantities
    representable as XSD or STMML dataTypes. The default layout is columnwise,
    with <ns1:tt>columns</ns1:tt> columns,
    where each column is a (homogeneous) <ns1:a href="el.array">array</ns1:a> of
    size <ns1:tt>rows</ns1:tt> data. This is the "normal" orientation of data tables
    but the table display could be transposed by XSLT transformation if required.
    Access is to columns, and thence to the data within them. DataTyping, delimiters,
    etc are delegated to the arrays, which must all be of the same size. For
    verification it is recommended that every array carries a <ns1:tt>size</ns1:tt>
    attribute.
    </ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>&lt;table rows="3" columns="2" title="people"&gt;
    &lt;array title="age" dataType="xsd:integer"&gt;3 5 7&lt;/array&gt;
    &lt;array title="name" dataType="xsd:string"&gt;Sue Fred Sandy&lt;/array&gt;
    &lt;/table&gt;
    </ns1:pre>
    </ns1:div>

    :ivar array:
    :ivar rows: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>Number of
        rows</ns1:p> </ns1:div>
    :ivar columns: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>Number of
        columns</ns1:p> </ns1:div>
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    """
    class Meta:
        name = "table"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    array: List[Array] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "min_occurs": 1,
        }
    )
    rows: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    columns: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )


@dataclass
class Unit:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A scientific unit</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>A scientific unit. Units are of the following types:</ns1:p>
    <ns1:ul>
    <ns1:li>SI Units. These may be one of the seven fundamental types
    (e.g. meter) or may be derived (e.g. joule). An SI unit is
    identifiable because it has no parentSI attribute and will have
    a unitType attribute.</ns1:li>
    <ns1:li>nonSI Units. These will normally have a parent SI unit
    (e.g. calorie has joule as an SI parent). </ns1:li>
    <ns1:li/>
    </ns1:ul>
    <ns1:p>Example:</ns1:p>
    <ns1:pre>
    &lt;unit id="units:fahr" name="fahrenheit" parentSI="units:K"
    multiplierToSI="0.55555555555555555"
    constantToSI="-17.777777777777777777"&gt;
    &lt;description&gt;An obsolescent unit of temperature still used in popular
    meteorology&lt;/description&gt;
    &lt;/unit&gt;
    </ns1:pre>
    </ns1:div>

    :ivar description:
    :ivar annotation:
    :ivar id:
    :ivar abbreviation:
    :ivar name:
    :ivar parent_si: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> A reference to the
        parent SI unit (forbidden for SI Units themselves). </ns1:div>
    :ivar unit_type: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> A reference to the
        unitType (required for SI Units). </ns1:div>
    :ivar multiplier_to_si: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>The factor by
        which the non-SI unit should be multiplied to convert a quantity
        to its representation in SI Units. This is applied
        <ns1:b>before</ns1:b><ns1:tt>constantToSI</ns1:tt>. Mandatory
        for nonSI units; forbidden for SI units</ns1:p> </ns1:div>
    :ivar constant_to_si: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>The amount to
        add to a quantity in non-SI units to convert its representation
        to SI Units. This is applied
        <ns1:b>after</ns1:b><ns1:tt>multiplierToSI</ns1:tt>. Optional
        for nonSI units; forbidden for SI units. </ns1:p> </ns1:div>
    :ivar deprecated_in_favor_of: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>The name of
        another unit that should be used in place of this unit.</ns1:p>
        </ns1:div>
    :ivar udunits_synonym: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>a synonym of
        this unit that from the dictionary used by the package udunits,
        written by unidata for conversions. See  http://unidata.ucar.edu
        Should not be used on a unit that is deprecated. </ns1:p>
        </ns1:div>
    """
    class Meta:
        name = "unit"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    description: List[Description] = field(
        default_factory=list,
        metadata={
            "type": "Element",
        }
    )
    annotation: List[Annotation] = field(
        default_factory=list,
        metadata={
            "type": "Element",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    abbreviation: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    parent_si: Optional[str] = field(
        default=None,
        metadata={
            "name": "parentSI",
            "type": "Attribute",
        }
    )
    unit_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "unitType",
            "type": "Attribute",
        }
    )
    multiplier_to_si: Decimal = field(
        default=Decimal("1"),
        metadata={
            "name": "multiplierToSI",
            "type": "Attribute",
        }
    )
    constant_to_si: Decimal = field(
        default=Decimal("0"),
        metadata={
            "name": "constantToSI",
            "type": "Attribute",
        }
    )
    deprecated_in_favor_of: Optional[str] = field(
        default=None,
        metadata={
            "name": "deprecatedInFavorOf",
            "type": "Attribute",
        }
    )
    udunits_synonym: Optional[str] = field(
        default=None,
        metadata={
            "name": "udunitsSynonym",
            "type": "Attribute",
        }
    )


@dataclass
class UnitType:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">An element containing the description of a scientific
    unit</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>Mandatory for SI Units,
    optional for nonSI units since they should be able to obtain this
    from their parent. For complex derived units without parents it may be
    useful.</ns1:p>
    <ns1:p>Used within a unitList</ns1:p>
    <ns1:p>Distinguish carefully from <ns1:a href="st.unitsType">unitsType</ns1:a>
    which is primarily used for attributes describing the units that elements
    carry</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>&lt;stm:unitList xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;!-- ========================= fundamental types =========================== --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;stm:unitType id="length" name="length"&gt;
    &lt;stm:dimension name="length"/&gt;
    &lt;/stm:unitType&gt;
    &lt;stm:unitType id="time" name="time"&gt;
    &lt;stm:dimension name="time"/&gt;
    &lt;/stm:unitType&gt;
    &lt;!-- ... --&gt;
    &lt;stm:unitType id="dimensionless" name="dimensionless"&gt;
    &lt;stm:dimension name="dimensionless"/&gt;
    &lt;/stm:unitType&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;!-- ========================== derived types ============================== --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;stm:unitType id="acceleration" name="acceleration"&gt;
    &lt;stm:dimension name="length"/&gt;
    &lt;stm:dimension name="time" power="-2"/&gt;
    &lt;/stm:unitType&gt;
    &lt;!-- ... --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;!-- ====================== fundamental SI units =========================== --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;stm:unit id="second" name="second" unitType="time"&gt;
    &lt;stm:description&gt;The SI unit of time&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;stm:unit id="meter" name="meter" unitType="length"
    abbreviation="m"&gt;
    &lt;stm:description&gt;The SI unit of length&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- ... --&gt;
    &lt;stm:unit id="kg" name="nameless" unitType="dimensionless"
    abbreviation="nodim"&gt;
    &lt;stm:description&gt;A fictitious parent for dimensionless units&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;!-- ===================== derived SI units ================================ --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;stm:unit id="newton" name="newton" unitType="force"&gt;
    &lt;stm:description&gt;The SI unit of force&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- ... --&gt;
    &lt;!-- multiples of fundamental SI units --&gt;
    &lt;stm:unit id="g" name="gram" unitType="mass"
    parentSI="kg"
    multiplierToSI="0.001"
    abbreviation="g"&gt;
    &lt;stm:description&gt;0.001 kg. &lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;stm:unit id="celsius" name="Celsius" parentSI="k"
    multiplierToSI="1"
    constantToSI="273.18"&gt;
    &lt;stm:description&gt;&lt;p&gt;A common unit of temperature&lt;/p&gt;&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- fundamental non-SI units --&gt;
    &lt;stm:unit id="inch" name="inch" parentSI="meter"
    abbreviation="in"
    multiplierToSI="0.0254" &gt;
    &lt;stm:description&gt;An imperial measure of length&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- derived non-SI units --&gt;
    &lt;stm:unit id="l" name="litre" unitType="volume"
    parentSI="meterCubed"
    abbreviation="l"
    multiplierToSI="0.001"&gt;
    &lt;stm:description&gt;Nearly 1 dm**3 This is not quite exact&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- ... --&gt;
    &lt;stm:unit id="fahr" name="fahrenheit" parentSI="k"
    abbreviation="F"
    multiplierToSI="0.55555555555555555"
    constantToSI="-17.777777777777777777"&gt;
    &lt;stm:description&gt;An obsolescent unit of temperature still used in popular
    meteorology&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;/stm:unitList&gt;
    </ns1:pre>
    </ns1:div>
    """
    class Meta:
        name = "unitType"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    dimension: List[Dimension] = field(
        default_factory=list,
        metadata={
            "type": "Element",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )


@dataclass
class Entry:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A dictionary entry</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;entry id="a003" term="alpha"
    dataType="float"
    minInclusive="0.0"
    maxInclusive="180.0"
    recommendedUnits="degrees"&gt;
    &lt;definition&gt;The alpha cell angle&lt;/definition&gt;
    &lt;/entry&gt;
    </ns1:pre>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;entry id="a003"
    term="matrix1"
    dataType="float"
    rows="3"
    columns="4"
    unitType="unit:length"
    minInclusive="0.0"
    maxInclusive="100.0"
    recommendedUnits="unit:m"
    totalDigits="8"
    fractionDigits="3"&gt;
    &lt;definition&gt;A matrix of lengths&lt;/definition&gt;
    &lt;description&gt;A data instance will have a matrix which points
    to this entry (e.g. dictRef="foo:matrix1"). The matrix must
    be 3*4, composed of floats in 8.3 format, of type length,
    values between 0 and 100 and with recommended units metres.
    &lt;/description&gt;
    &lt;/entry&gt;
    </ns1:pre>
    </ns1:div>

    :ivar definition:
    :ivar alternative:
    :ivar annotation:
    :ivar description:
    :ivar enumeration:
    :ivar related_entry:
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar data_type:
    :ivar rows:
    :ivar columns:
    :ivar recommended_units:
    :ivar unit_type:
    :ivar min_exclusive:
    :ivar min_inclusive:
    :ivar max_exclusive:
    :ivar max_inclusive:
    :ivar total_digits:
    :ivar fraction_digits:
    :ivar length:
    :ivar min_length:
    :ivar max_length:
    :ivar units:
    :ivar white_space:
    :ivar pattern:
    :ivar term:
    """
    class Meta:
        name = "entry"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    definition: Optional[Definition] = field(
        default=None,
        metadata={
            "type": "Element",
        }
    )
    alternative: List[Alternative] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "sequential": True,
        }
    )
    annotation: List[Annotation] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "sequential": True,
        }
    )
    description: List[Description] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "sequential": True,
        }
    )
    enumeration: List[Enumeration] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "sequential": True,
        }
    )
    related_entry: List[RelatedEntry] = field(
        default_factory=list,
        metadata={
            "name": "relatedEntry",
            "type": "Element",
            "sequential": True,
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    data_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "dataType",
            "type": "Attribute",
        }
    )
    rows: int = field(
        default=1,
        metadata={
            "type": "Attribute",
        }
    )
    columns: int = field(
        default=1,
        metadata={
            "type": "Attribute",
        }
    )
    recommended_units: Optional[str] = field(
        default=None,
        metadata={
            "name": "recommendedUnits",
            "type": "Attribute",
        }
    )
    unit_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "unitType",
            "type": "Attribute",
        }
    )
    min_exclusive: Optional[Decimal] = field(
        default=None,
        metadata={
            "name": "minExclusive",
            "type": "Attribute",
        }
    )
    min_inclusive: Optional[Decimal] = field(
        default=None,
        metadata={
            "name": "minInclusive",
            "type": "Attribute",
        }
    )
    max_exclusive: Optional[Decimal] = field(
        default=None,
        metadata={
            "name": "maxExclusive",
            "type": "Attribute",
        }
    )
    max_inclusive: Optional[Decimal] = field(
        default=None,
        metadata={
            "name": "maxInclusive",
            "type": "Attribute",
        }
    )
    total_digits: Optional[int] = field(
        default=None,
        metadata={
            "name": "totalDigits",
            "type": "Attribute",
        }
    )
    fraction_digits: Optional[int] = field(
        default=None,
        metadata={
            "name": "fractionDigits",
            "type": "Attribute",
        }
    )
    length: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    min_length: Optional[int] = field(
        default=None,
        metadata={
            "name": "minLength",
            "type": "Attribute",
        }
    )
    max_length: Optional[int] = field(
        default=None,
        metadata={
            "name": "maxLength",
            "type": "Attribute",
        }
    )
    units: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    white_space: Optional[str] = field(
        default=None,
        metadata={
            "name": "whiteSpace",
            "type": "Attribute",
        }
    )
    pattern: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    term: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )


@dataclass
class UnitList:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A container for several unit entries</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">Usually forms the complete units dictionary (along with metadata)</ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>&lt;stm:unitList xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;!-- ========================= fundamental types =========================== --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;stm:unitType id="length" name="length"&gt;
    &lt;stm:dimension name="length"/&gt;
    &lt;/stm:unitType&gt;
    &lt;stm:unitType id="time" name="time"&gt;
    &lt;stm:dimension name="time"/&gt;
    &lt;/stm:unitType&gt;
    &lt;!-- ... --&gt;
    &lt;stm:unitType id="dimensionless" name="dimensionless"&gt;
    &lt;stm:dimension name="dimensionless"/&gt;
    &lt;/stm:unitType&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;!-- ========================== derived types ============================== --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;stm:unitType id="acceleration" name="acceleration"&gt;
    &lt;stm:dimension name="length"/&gt;
    &lt;stm:dimension name="time" power="-2"/&gt;
    &lt;/stm:unitType&gt;
    &lt;!-- ... --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;!-- ====================== fundamental SI units =========================== --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;stm:unit id="second" name="second" unitType="time"&gt;
    &lt;stm:description&gt;The SI unit of time&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;stm:unit id="meter" name="meter" unitType="length"
    abbreviation="m"&gt;
    &lt;stm:description&gt;The SI unit of length&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- ... --&gt;
    &lt;stm:unit id="kg" name="nameless" unitType="dimensionless"
    abbreviation="nodim"&gt;
    &lt;stm:description&gt;A fictitious parent for dimensionless units&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;!-- ===================== derived SI units ================================ --&gt;
    &lt;!-- ======================================================================= --&gt;
    &lt;stm:unit id="newton" name="newton" unitType="force"&gt;
    &lt;stm:description&gt;The SI unit of force&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- ... --&gt;
    &lt;!-- multiples of fundamental SI units --&gt;
    &lt;stm:unit id="g" name="gram" unitType="mass"
    parentSI="kg"
    multiplierToSI="0.001"
    abbreviation="g"&gt;
    &lt;stm:description&gt;0.001 kg. &lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;stm:unit id="celsius" name="Celsius" parentSI="k"
    multiplierToSI="1"
    constantToSI="273.18"&gt;
    &lt;stm:description&gt;&lt;p&gt;A common unit of temperature&lt;/p&gt;&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- fundamental non-SI units --&gt;
    &lt;stm:unit id="inch" name="inch" parentSI="meter"
    abbreviation="in"
    multiplierToSI="0.0254" &gt;
    &lt;stm:description&gt;An imperial measure of length&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- derived non-SI units --&gt;
    &lt;stm:unit id="l" name="litre" unitType="volume"
    parentSI="meterCubed"
    abbreviation="l"
    multiplierToSI="0.001"&gt;
    &lt;stm:description&gt;Nearly 1 dm**3 This is not quite exact&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;!-- ... --&gt;
    &lt;stm:unit id="fahr" name="fahrenheit" parentSI="k"
    abbreviation="F"
    multiplierToSI="0.55555555555555555"
    constantToSI="-17.777777777777777777"&gt;
    &lt;stm:description&gt;An obsolescent unit of temperature still used in popular
    meteorology&lt;/stm:description&gt;
    &lt;/stm:unit&gt;
    &lt;/stm:unitList&gt;
    </ns1:pre>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>&lt;stm:unitList
    xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"
    dictRef="unit" href="units.xml" /&gt;
    </ns1:pre>
    </ns1:div>

    :ivar unit_type:
    :ivar unit:
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar href: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">Maps a <ns1:a
        href="st.dictRefType">dictRef</ns1:a> prefix to the location of
        a dictionary.</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description">This requires the
        prefix and the physical URI address to be contained within the
        same file. We can anticipate that better mechanisms will arise -
        perhaps through XMLCatalogs. At least it works at
        present.</ns1:div>
    """
    class Meta:
        name = "unitList"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    unit_type: List[UnitType] = field(
        default_factory=list,
        metadata={
            "name": "unitType",
            "type": "Element",
        }
    )
    unit: List[Unit] = field(
        default_factory=list,
        metadata={
            "type": "Element",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    href: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )


@dataclass
class Dictionary:
    """<ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
    class="summary">A dictionary</ns1:div>

    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="description">
    <ns1:p>A dictionary is a container for <ns1:a href="el.entry">entry</ns1:a>
    elements. Dictionaries can also contain unit-related information.</ns1:p>
    <ns1:p>The dictRef attribute on a <ns1:tt>dictionary</ns1:tt> element
    sets a namespace-like prefix allowing the dictionary to be referenced from
    within the document. In general dictionaries are
    referenced from an element using the
    <ns1:a href="gp.dictRefGroup">dictRef</ns1:a> attribute.</ns1:p>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:pre>
    &lt;stm:dictionary xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
    &lt;stm:entry id="a001" term="Amplitude for charge density mixing"
    dataType="stm:decimal"
    units="arbitrary"&gt;
    &lt;stm:annotation&gt;
    &lt;stm:documentation&gt;
    &lt;div class="summary"&gt;Amplitude for charge density mixing&lt;/div&gt;
    &lt;div class="description"&gt;Not yet filled in...&lt;/div&gt;
    &lt;/stm:documentation&gt;
    &lt;/stm:annotation&gt;
    &lt;stm:alternative type="abbreviation"&gt;CDMixAmp&lt;/stm:alternative&gt;
    &lt;/stm:entry&gt;
    &lt;/stm:dictionary&gt;
    </ns1:pre>
    </ns1:div>
    <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2" class="example">
    <ns1:p><ns1:tt>dictionary</ns1:tt> can be used in an instance
    document to reference the dictionary used. Example:</ns1:p>
    <ns1:pre>
    &lt;list&gt;
    &lt;dictionary
    dictRef="core" href="../dictionary/coreDict.xml"/&gt;
    &lt;/list&gt;
    </ns1:pre>
    </ns1:div>

    :ivar unit_list: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>This is so
        that a reference to a UnitList can be put in a
        dictionary.</ns1:p> </ns1:div>
    :ivar annotation:
    :ivar description:
    :ivar entry:
    :ivar id: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A attribute prividing
        a unique ID for STM elements</ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>See <ns1:a
        href="st.idType">idType</ns1:a> for full documentation.</ns1:p>
        </ns1:div>
    :ivar title: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">An (optional) title on
        most STM elements. Uncontrolled</ns1:div>
    :ivar convention: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">A reference to a
        convention</ns1:div> <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="description"> <ns1:p>A
        reference to a convention which is inherited by all the
        subelements.</ns1:p> <ns1:p>It may be useful to create
        conventions with namespaces, </ns1:p> <ns1:p>this attribute is
        inherited by its child elements; thus a
        <ns1:tt>molecule</ns1:tt> with a convention sets the default for
        its bonds and atoms. This can be overwritten if necessary by an
        explicit <ns1:tt>convention</ns1:tt>.</ns1:p> <ns1:p>Use of
        convention will normally require non-STM-ML semantics, and
        should be used with caution. We would expect that conventions
        prefixed with "ISO" would be useful, such as ISO8601 for
        dateTimes.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;bond convention="fooChem"
        order="-5" xmlns:fooChem="http://www.fooChem/conventions"/&gt;
        </ns1:pre> </ns1:div>
    :ivar dict_ref: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary"> <ns1:p>A reference to
        a dictionary entry.</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="description"> <ns1:p>Elements in data instances such as
        <ns1:a href="el.scalar">scalar</ns1:a> may have a
        <ns1:tt>dictRef</ns1:tt> attribute to point to an entry in a
        dictionary. To avoid excessive use of (mutable) filenames and
        URIs we recommend a namespace prefix, mapped to a namespace URI
        in the normal manner. In this case, of course, the namespace URI
        must point to a real XML document containing <ns1:a
        href="el.entry">entry</ns1:a> elements and validated against
        STM-ML Schema.</ns1:p> <ns1:p>Where there is concern about the
        dictionary becoming separated from the document the dictionary
        entries can be physically included as part of the data instance
        and the normal XPointer addressing mechanism can be
        used.</ns1:p> <ns1:p>This attribute can also be used on <ns1:a
        href="el.dictionary">dictionary</ns1:a> elements to define the
        namespace prefix</ns1:p> </ns1:div> <ns1:div
        xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;scalar dataType="xsd:float"
        title="surfaceArea" dictRef="cmlPhys:surfArea"
        xmlns:cmlPhys="http://www.xml-cml.org/dict/physical"
        units="units:cm2"&gt;50&lt;/scalar&gt; </ns1:pre> </ns1:div>
        <ns1:div xmlns:ns1="http://www.xml-cml.org/schema/stmml-1.2"
        class="example"> <ns1:pre> &lt;stm:list
        xmlns:stm="http://www.xml-cml.org/schema/stmml-1.1"&gt;
        &lt;stm:observation&gt; &lt;p&gt;We observed &lt;object
        count="3" dictRef="#p1"/&gt; constructing dwellings of different
        material&lt;/p&gt; &lt;/stm:observation&gt; &lt;stm:entry
        id="p1" term="pig"&gt; &lt;stm:definition&gt;A domesticated
        animal.&lt;/stm:definition&gt; &lt;stm:description&gt;Predators
        include wolves&lt;/stm:description&gt; &lt;stm:description
        class="scientificName"&gt;Sus scrofa&lt;/stm:description&gt;
        &lt;/stm:entry&gt; &lt;/stm:list&gt; </ns1:pre> </ns1:div>
    :ivar href: <ns1:div xmlns:ns1="http://www.xml-
        cml.org/schema/stmml-1.2" class="summary">URI giving the
        location of the document. Mandatory if <ns1:tt>dictRef</ns1:tt>
        present. </ns1:div>
    """
    class Meta:
        name = "dictionary"
        namespace = "http://www.xml-cml.org/schema/stmml-1.2"

    unit_list: List[UnitList] = field(
        default_factory=list,
        metadata={
            "name": "unitList",
            "type": "Element",
        }
    )
    annotation: List[Annotation] = field(
        default_factory=list,
        metadata={
            "type": "Element",
        }
    )
    description: List[Description] = field(
        default_factory=list,
        metadata={
            "type": "Element",
        }
    )
    entry: List[Entry] = field(
        default_factory=list,
        metadata={
            "type": "Element",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z0-9_\-]+(:[A-Za-z0-9_\-]+)?",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    convention: str = field(
        default="CML",
        metadata={
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    dict_ref: Optional[str] = field(
        default=None,
        metadata={
            "name": "dictRef",
            "type": "Attribute",
            "pattern": r"[A-Za-z][A-Za-z0-9_]*(:[A-Za-z][A-Za-z0-9_]*)?",
        }
    )
    href: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
