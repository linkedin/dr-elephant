import xml.etree.ElementTree as elt
tree = elt.parse('../app-conf/HeuristicConf.xml')
root = tree.getroot()
import sys

from xml.etree import ElementTree
XML = """  <heuristic>
    <applicationtype>mapreduce</applicationtype>
    <heuristicname>Hello World! heuristic</heuristicname>
    <classname>com.linkedin.drelephant.mapreduce.heuristics.HelloWorldHeuristic</classname>
    <viewname>views.html.help.mapreduce.helpHelloWorld</viewname>
</heuristic>  """

appliName=sys.argv[1]  #"mapreduce"
heuristicName=sys.argv[2]  #"heuristicV2"
className="com.linkedin.drelephant."#HeuristicV2
viewName="views.html.help"#heuristic_V2


className = className + appliName + ".heuristics." + sys.argv[3]
viewName=viewName + "." + appliName + "." + sys.argv[4]


et = ElementTree.fromstring(XML)

content = et.findall('applicationtype')
content[0].text = appliName
content = et.findall('heuristicname')
content[0].text = heuristicName
content = et.findall('classname')
content[0].text = className
content = et.findall('viewname')
content[0].text = viewName

from xml.etree.ElementTree import Element, SubElement, Comment, tostring
print "updating ../app-conf/HeuristicConf.xml ...."
root.append(et);
tree.write('../app-conf/HeuristicConf.xml')

