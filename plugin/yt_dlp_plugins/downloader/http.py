from yt_dlp.downloader.http import HttpFD as OriginalHttpFD


class HttpFD(OriginalHttpFD):
    def real_download(self, filename, info_dict):
        return super().real_download(filename, info_dict)
