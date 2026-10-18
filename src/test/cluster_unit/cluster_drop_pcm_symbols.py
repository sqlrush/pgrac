# Only isolates test kernel fixtures; production PCM source is not rewritten.
from pathlib import Path
import subprocess,sys
names=set()
for p in sys.argv[2:]:
 cmd=['nm','-gU',p] if sys.platform=='darwin' else ['nm','-g','--defined-only',p]
 for line in subprocess.check_output(cmd,text=True).splitlines():
  parts=line.split()
  if len(parts)!=3 or parts[1] not in 'TDBSCWV':continue
  name=parts[2]
  if sys.platform=='darwin':name=name[1:] if name.startswith('_') else name
  if name.isidentifier() and not name.startswith('z_pcm_bridge_'):names.add(name)
# This fixture temporarily overrides/undefines this API around a generated
# consumer. It has no competing definition in the KO fixture.
names.discard('cluster_pcm_lock_resource_x_ready_intent_probe_exact')
assert len(names)>100
Path(sys.argv[1]).write_text('/* Generated: isolated test runtime symbols. */\n'+''.join('#define '+s+' z_pcm_isolated_'+s+'\n' for s in sorted(names)))
