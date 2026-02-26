// pipeline/docs/report.typ
// Psalms NLP Analysis — Typst PDF template
//
// Compiled by: typst compile report.typ /data/outputs/report.pdf
// Pre-requisite: analysis.ipynb must have been executed and figures saved to
//   /data/outputs/figures/

#import "@preview/charged-ieee:0.1.3": ieee

#show: ieee.with(
  title: [Psalms NLP: Quantifying Translation Fidelity to Hebrew Phonetic Structure],
  authors: (
    (name: "Psalms NLP Pipeline", organization: "Self-hosted"),
  ),
  abstract: [
    This report presents a computational analysis of the Hebrew Psalms corpus
    using morphological and phonetic fingerprinting to quantify style deviation
    and breath alignment across configured English translations. Four dimensions
    are measured per verse: syllable density, morpheme ratio, sonority score,
    and clause compression. Deviation scores and breath alignment metrics are
    stored in PostgreSQL and visualised as heatmaps, radar charts, and arc
    diagrams.
  ],
)

= Introduction

The analysis is conducted using the BHSA (Biblia Hebraica Stuttgartensia
Amstelodamensis) morphological database @bhsa2021, which provides word-level
annotation for the entire Hebrew Bible. Each verse in the Psalms is characterised
by a four-dimensional fingerprint vector and a per-syllable breath weight curve.
English translations are scored against these fingerprints to produce composite
deviation and breath alignment metrics.

= Methodology

Syllable density is computed from the BHSA syllable segmentation. Morpheme ratio
is derived from the ratio of roots to total word tokens. Sonority score aggregates
vowel openness values from the syllable token table. Clause compression quantifies
the number of syntactic clauses per colon.

Breath weight curves map syllable-level phonetic weight to a normalised [0, 1]
interval. Translation stress positions are estimated via English syllabification
and compared to the Hebrew curve using Pearson correlation.

= Results

== Style Deviation Heatmap

#figure(
  image("/data/outputs/figures/deviation_heatmap.png", width: 100%),
  caption: [
    Mean style deviation by Psalm chapter and translation. Red cells indicate
    high composite deviation; green cells indicate close alignment to the
    Hebrew fingerprint.
  ],
)

== Breath Curve — Psalm 23:1

#figure(
  image("/data/outputs/figures/breath_sample.png", width: 80%),
  caption: [
    Per-syllable breath weight curve for Psalm 23:1 (Hebrew source).
    Relative position on the x-axis normalises verse length to [0, 1].
  ],
)

= Discussion

Translations with lower composite deviation values more closely mirror the
phonetic and structural density of the Hebrew source. Breath alignment scores
above 0.7 indicate reasonable stress correspondence. Results should be interpreted
alongside traditional scholarly commentary @alter2007 @watson1984 — computational
metrics are descriptive, not prescriptive.

= Conclusion

The Psalms NLP pipeline provides reproducible, quantitative fingerprints for
every verse in the Psalter and scores any configured English translation against
them. The pipeline is config-driven and designed for expansion to other Hebrew
Bible books.

#bibliography("references.bib")
