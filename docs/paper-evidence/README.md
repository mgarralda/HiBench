# Paper evidence — HiBench Next v2.5.0

The [evidence archive](HiBench-Next-v2.5.0-paper-evidence.zip) contains the factual
records behind the paper's SVD Large, PCA Large, Repartition Small and Sleep
Small comparisons. It is separate from the archived software release.

The approximately 11 MB ZIP includes:

- 34 original Spark event logs: 24 measured applications, eight warmups and two
  dataset preparations.
- Effective configurations, run results, console logs, input manifests and checks,
  software/native-library identities, and physical plans.
- Parsed task/stage/application CSVs, three-repetition summaries and numerical
  checks for SVD singular values and PCA explained variance.
- Final figures, complete-sequence supplements, captions, and analysis scripts
  with an offline regeneration helper.

The ZIP's internal `SHA256SUMS` covers its contents. The
[archive checksum](HiBench-Next-v2.5.0-paper-evidence.zip.sha256) identifies the
ZIP itself. Large input datasets, working databases, duplicate binaries and
unrelated experiments are excluded; input-generation conditions and checksums
are retained. The archive README documents the protocol, versions and limits.

Figures 4 and 5 use the first measured pair. PCA raw/min-max panels show a
labelled zoom of 40 of the 1,047 tasks; CUSUM and polynomial fits use the whole
application. Supplementary figures preserve the complete sequences. The detail
window is an illustration chosen after the campaign, not a representative sample.

## Regenerate offline

Extract the ZIP, install the NumPy/Matplotlib versions listed in its README, and
run from the extracted directory:

```sh
python reproduce.py --output regenerated
```

No cluster is needed to reparse logs and regenerate tables and figures. Rerunning
the experiments requires the separately documented Spark/Hadoop laboratory.

## Reference from the paper

Suggested availability statement:

> The experimental evidence is available in the HiBench Next repository under
> `docs/paper-evidence/HiBench-Next-v2.5.0-paper-evidence.zip`. It includes original
> Spark event logs, effective configurations, dataset checks, parsed observations,
> physical plans, numerical validation, and scripts to regenerate the tables and
> figures. The archive also contains the complete task-profile figures and the
> captions for the displayed PCA details. The software release is archived
> separately at https://doi.org/10.5281/zenodo.23400663.

Repository link:
[experimental evidence](https://github.com/mgarralda/HiBench-Next/tree/main/docs/paper-evidence).
After publishing, cite a link pinned to the commit containing this archive,
alongside its SHA-256, so the paper identifies a fixed artifact. The software
`v2.5.0` tag predates this evidence addition; do not use that tag in its URL.
This preparation does not itself upload the archive or assign an evidence DOI.
