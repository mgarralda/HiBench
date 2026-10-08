# HiBench Next namespace

Project packages formerly named `com.intel.hibench` now use `org.hibench`.
Java and Scala source directories match their declared packages, including the
optional XGBoost sources. Launch scripts, Spark's benchmark listener and Maven
group IDs use the new namespace. Artifact filenames remain unchanged.

Third-party packages and copyright/license notices retain their original names.
This includes Intel DAAL and the bundled Hadoop, Mahout and Nutch sources.
Older generators under the existing `HiBench` package are outside this Intel
namespace migration.

Rebuild and reinstall scripts and JARs together before launching a workload.
Custom submissions must replace `com.intel.hibench.*` with `org.hibench.*`.
No compatibility aliases are supplied for the old class names. Historical audit
and validation reports retain their original paths, class names and hashes.
Serialized objects that contain renamed class names may require regeneration or
an explicit import; this migration does not certify old serialized datasets.

The package rename changes code organization, not generator distributions or
workload algorithms.
