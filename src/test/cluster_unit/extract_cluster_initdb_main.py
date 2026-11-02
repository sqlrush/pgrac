import pathlib,re,sys
s=pathlib.Path(sys.argv[1]).read_text(); base=pathlib.Path(sys.argv[2])
def one(pattern):
 m=re.findall(pattern,s,re.M|re.S)
 if len(m)!=1: raise SystemExit(pattern)
 return m[0]
types=[one(r"typedef struct "+n+r"\s*\{.*?\} "+n+r";") for n in ["InitdbDirectory","InitdbOrigin","InitdbRegistration"]]
helpers=[one(r"(?:static )?(?:bool|void)\s+"+n+r"\(.*?^\}") for n in ["cluster_initdb_cohort_relmap_prepare_v2","cluster_initdb_cohort_prepare_v2","creation_relmap_registry_valid","creation_boot_registry_valid","creation_registration_build"]]
for part,text in [("types","\n".join(types)),("helpers","\n".join(helpers)),("body",one(r"void\nClusterInitdbCohortMain\(.*?^\}"))]:
 base.with_name(base.name+"_"+part+".inc").write_text(text+"\n")
