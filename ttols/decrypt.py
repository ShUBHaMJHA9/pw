import os
from mainLogic.error import CouldNotDecryptAudio, CouldNotDecryptVideo, DependencyNotFound
from mainLogic.utils.glv_var import debugger
from mainLogic.utils.basicUtils import BasicUtils
from mainLogic.utils.process import shell

class Decrypt:

    def _decrypt(self, path, name, key, out_type="Video", mp4d="mp4decrypt", outfile=None, outdir=None, suppress_exit=False):
        path = BasicUtils.abspath(path)
        file_path = os.path.join(outdir or path, f"{outfile+'-' if outfile else ''}{out_type}.mp4")

        if shell(mp4d) > 1:
            debugger.error(DependencyNotFound("Mp4decrypt"))
            return None

        code = shell([mp4d, "--key", f"1:{key}", f"{path}/{name}.mp4", file_path], verbose=True)

        if code == 0:
            return os.path.abspath(file_path)

        if os.path.exists(file_path):
            os.remove(file_path)

        err = CouldNotDecryptAudio if out_type == "Audio" else CouldNotDecryptVideo
        debugger.error(err())
        if not suppress_exit:
            err().exit()

    decryptAudio = lambda self, path, name, key, **kwargs: self._decrypt(path, name, key, "Audio", **kwargs)
    decryptVideo = lambda self, path, name, key, **kwargs: self._decrypt(path, name, key, "Video", **kwargs)
