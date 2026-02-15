import os
from mainLogic.error import errorList, OverwriteAbortedByUser
from mainLogic.utils.glv_var import debugger
from mainLogic.utils.process import shell
from mainLogic.utils.glv import Global
from mainLogic.utils.os2 import SysFunc
class Merge:
    """
    Slytherin is known for resourcefulness and ambition, ensuring everything is left in a state that suits their
    needs. The cleanup phase is about efficiency, leaving no trace behind, just like a Slytherin covering their tracks.
    """

    def mergeCommandBuilder(self,ffmpeg_path,input1,input2,output,overwrite=False):

    #    return f'{ffmpeg_path} {"-y" if overwrite else ""} -i {input1} -i {input2} -c copy {output}'
        return [
            ffmpeg_path,
            "-y" if overwrite else "",
            "-i", input1,
            "-i", input2,
            "-c", "copy",
            output
        ]


    def ffmpegMerge(self, input1, input2, output, ffmpeg_path="ffmpeg", verbose=False, use_gpu=False):

        input1, input2, output = SysFunc.modify_path(input1), SysFunc.modify_path(input2), SysFunc.modify_path(output)

        if verbose:
            Global.hr(); debugger.debug('Attempting ffmpeg merge')

        if os.path.exists(output):
            debugger.error("Warning: Output file already exists. Overwriting...")
            consent = input("Do you want to continue? (y/n): ")
            if consent.lower() != 'y':
                OverwriteAbortedByUser().exit()

        # Build ffmpeg command. By default, perform a stream copy which is fastest.
        if use_gpu:
            # When GPU is requested, re-encode video using NVENC if available and copy audio.
            cmd = [
                ffmpeg_path,
                "-y",
                # use CUDA hwaccel for faster decode if available
                "-hwaccel", "cuda",
                "-i", input1,
                "-i", input2,
                "-map", "0:v:0",
                "-map", "1:a:0",
                "-c:v", "h264_nvenc",
                "-preset", "p7",
                "-rc", "vbr_hq",
                "-cq", "19",
                "-b:v", "0",
                "-c:a", "copy",
                output,
            ]
        else:
            # Fastest: stream copy
            cmd = [
                ffmpeg_path,
                "-y",
                "-i", input1,
                "-i", input2,
                "-c", "copy",
                output,
            ]

        if verbose:
            debugger.debug(f"Running: {cmd}")
            shell(cmd, filter='.*')
        else:
            shell(cmd, stderr="", stdout="")

        return output