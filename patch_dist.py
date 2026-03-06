import zipfile, shutil, os, tarfile, io

def patch():
    orig='dist/zsdtdx-0.1.0-py3-none-any.whl'
    new='dist/zsdtdx-1.1.1-py3-none-any.whl'
    shutil.copy(orig,new)
    with zipfile.ZipFile(new,'a') as z:
        meta_name=[n for n in z.namelist() if n.endswith('METADATA')][0]
        data=z.read(meta_name).decode().splitlines()
        newdata=[line if not line.startswith('Version:') else 'Version: 1.1.1' for line in data]
        z.writestr(meta_name,'\n'.join(newdata))
    print('patched wheel', new)
    orig_tgz='dist/zsdtdx-0.1.0.tar.gz'
    new_tgz='dist/zsdtdx-1.1.1.tar.gz'
    with tarfile.open(orig_tgz,'r:gz') as tar:
        members=tar.getmembers()
        with tarfile.open(new_tgz,'w:gz') as tar2:
            for m in members:
                f=tar.extractfile(m)
                if f and m.name.endswith('PKG-INFO'):
                    content=f.read().decode().splitlines()
                    content=[line if not line.startswith('Version:') else 'Version: 1.1.1' for line in content]
                    info=tarfile.TarInfo(name=m.name)
                    info.size=len('\n'.join(content).encode())
                    tar2.addfile(info, io.BytesIO('\n'.join(content).encode()))
                else:
                    if f:
                        tar2.addfile(m,f)
                    else:
                        tar2.addfile(m)
    print('patched sdist', new_tgz)

if __name__=='__main__':
    patch()
