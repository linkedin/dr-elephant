BUILD_PATH=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Enable analytics in the conf file.
sed -i.bak 's/^enable_analytics=false$/enable_analytics=true/' $BUILD_PATH/app-conf/elephant.conf
rm -rf "$BUILD_PATH"/app-conf/elephant.conf.bak

cd $BUILD_PATH/

# Compile the Dr Elephant code
bash ./compile.sh

# Generate the pom file (other artifacts are disabled) and publish to bintray
sbt clean 'set publishArtifact in (Compile, packageBin) := false' 'set publishArtifact in (Compile, packageDoc) := false' 'set publishArtifact in (Compile, packageSrc) := false' publish

MODULE=dr-elephant_2.10

VERSION=`sed -n 's/.*version := \"\([^"]*\)\".*/\1/p' $BUILD_PATH/build.sbt`

mv $BUILD_PATH/dist/dr-elephant*.zip $BUILD_PATH/dist/$MODULE-$VERSION.zip

cd dist/

echo "Uploading the distribution to bintray"
curl -T $MODULE-"$VERSION".zip -u"$1":"$2" https://api.bintray.com/content/linkedin/maven/dr-elephant/"$VERSION"/com/linkedin/drelephant/$MODULE/"$VERSION"/ --verbose

echo "Publishing the uploaded distribution"
curl -X POST -u"$1":"$2" https://api.bintray.com/content/linkedin/maven/dr-elephant/"$VERSION"/publish --verbose