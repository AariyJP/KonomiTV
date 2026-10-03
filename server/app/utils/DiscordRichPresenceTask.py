
import asyncio
import contextlib
from typing import Any
from urllib.parse import quote

from app import logging
from app.config import ClientSettings
from app.constants import DISCORD_CLIENT_ID, LOGO_DIR
from app.models.Channel import Channel
from app.models.User import User
from app.streams.LiveStream import LiveStream
from app.utils.DiscordRPCClient import DiscordRPCClient


class DiscordRichPresenceTask:
    """
    視聴中のテレビ番組を、KonomiTV サーバーと同じ PC で起動している Discord の Rich Presence に定期的に反映するタスク
    """

    # アクティビティを更新する間隔 (秒)
    UPDATE_INTERVAL = 15

    # 局ロゴの公開 URL のベース
    ## Discord は外部 URL の画像をアクティビティに表示できるが、KonomiTV サーバーはインターネットから到達できないとは限らないため、
    ## GitHub 上にある同梱ロゴ (server/static/logos/) の URL を使う
    LOGO_BASE_URL = 'https://raw.githubusercontent.com/tsukumijima/KonomiTV/master/server/static/logos/'

    # KonomiTV のアイコンの公開 URL
    KONOMITV_ICON_URL = 'https://raw.githubusercontent.com/tsukumijima/KonomiTV/master/client/public/assets/images/icons/icon-192px.png'


    def __init__(self) -> None:
        """
        DiscordRichPresenceTask を初期化する
        """

        # Discord クライアントとの IPC 接続を管理するクライアント
        ## _update() で接続・アクティビティ設定を行い、stop() で接続を閉じる
        self._client = DiscordRPCClient(DISCORD_CLIENT_ID)

        # アクティビティを定期的に更新する asyncio タスク
        ## start() で生成され、stop() でキャンセルされる (未開始・停止済みの場合は None)
        self._task: asyncio.Task[None] | None = None

        # 最後に Discord に設定したアクティビティ
        ## _update() で前回と同じ内容のアクティビティを何度も送信しないために参照する
        ## 未設定の場合や再接続した直後は None になる
        self._last_activity: dict[str, Any] | None = None


    async def start(self) -> None:
        """
        アクティビティの定期更新を開始する
        """

        if self._task is None:
            self._task = asyncio.create_task(self._run())


    async def stop(self) -> None:
        """
        アクティビティの定期更新を停止し、Discord クライアントとの接続を閉じる
        """

        # 定期更新タスクをキャンセルし、終了を待つ
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

        # 接続を閉じると、Discord 側でアクティビティが消去される
        await self._client.close()


    async def _run(self) -> None:
        """
        UPDATE_INTERVAL 秒ごとにアクティビティを更新し続ける
        """

        while True:
            # 更新中に予期しないエラーが発生しても定期更新自体は止めない
            try:
                await self._update()
            except Exception as ex:
                logging.error('[DiscordRichPresenceTask] Failed to update activity:', exc_info=ex)
            await asyncio.sleep(self.UPDATE_INTERVAL)


    async def _update(self) -> None:
        """
        現在の視聴状況に応じて、Discord のアクティビティを1回更新する
        """

        # 視聴中のクライアントがいるライブストリームのうち、最初の1つを対象にする
        ## KonomiTV サーバーではどのユーザーが視聴しているかは区別できないため、複数ある場合は1つだけ表示する
        ## メモリ上の情報だけで判定できるため、DB へのアクセスや Discord への接続より先に行う
        live_stream = next((
            live_stream for live_stream in LiveStream.getAllLiveStreams()
            if live_stream.getStatus().client_count > 0
        ), None)

        # サーバーに同期されたクライアント設定のうち、いずれかのユーザーで discord_rich_presence がオンなら有効とみなす
        ## 誰も視聴していない場合は設定に関わらず表示するものがないため、DB へのアクセスを省略する
        is_enabled = live_stream is not None and any(
            ClientSettings.model_validate(user.client_settings).discord_rich_presence for user in await User.all()
        )

        # 誰も視聴していないか設定で無効になっている場合、接続中であれば切断してアクティビティを消去する
        if live_stream is None or is_enabled is False:
            if self._client.is_connected is True:
                await self._client.close()
            return

        # 未接続なら接続を試みる (Discord が起動していなければ次回に持ち越す)
        ## 再接続した場合、Discord 側のアクティビティは消えているため、前回の送信内容をリセットして必ず送り直す
        if self._client.is_connected is False:
            if await self._client.connect() is False:
                return
            self._last_activity = None

        # 前回送信した内容から変化があったときだけ送信する
        activity = await self._getActivity(live_stream)
        if activity != self._last_activity and await self._client.setActivity(activity) is True:
            self._last_activity = activity


    async def _getActivity(self, live_stream: LiveStream) -> dict[str, Any] | None:
        """
        視聴中のライブストリームから、Discord に設定するアクティビティを生成する

        Args:
            live_stream (LiveStream): 視聴中のライブストリーム

        Returns:
            dict[str, Any] | None: アクティビティ (チャンネル情報が見つからない場合は None)
        """

        channel = await Channel.filter(display_channel_id=live_stream.display_channel_id).first()
        if channel is None:
            return None

        # 現在放送中の番組と、同梱されている局ロゴを取得する
        program_present, _ = await channel.getCurrentAndNextProgram()
        logo_path = await channel.getBundledLogoFilePath()

        # 同梱の局ロゴがあれば大きい画像に局ロゴ、小さい画像に KonomiTV のアイコンを表示する
        ## ロゴのファイル名には全角文字を含むものがあるため、URL エンコードしておく
        if logo_path is not None:
            assets = {
                'large_image': self.LOGO_BASE_URL + quote(logo_path.relative_to(LOGO_DIR).as_posix()),
                'large_text': channel.name,
                'small_image': self.KONOMITV_ICON_URL,
                'small_text': 'KonomiTV',
            }
        # 同梱の局ロゴがなければ、大きい画像に KonomiTV のアイコンを表示する
        else:
            assets = {
                'large_image': self.KONOMITV_ICON_URL,
                'large_text': 'KonomiTV',
            }

        # アクティビティを生成する
        ## type: 3 は「Watching (視聴中)」を表す
        ## details / state は 2 〜 128 文字である必要があるため、切り詰めた上で 2 文字未満なら空白で埋める
        activity: dict[str, Any] = {
            'type': 3,
            'details': (program_present.title if program_present is not None else channel.name)[:128].ljust(2),
            'state': channel.name[:128].ljust(2),
            'assets': assets,
        }

        # 番組情報があれば、番組の開始・終了時刻 (UNIX ミリ秒) を設定して経過時間・残り時間を表示する
        if program_present is not None:
            activity['timestamps'] = {
                'start': int(program_present.start_time.timestamp() * 1000),
                'end': int(program_present.end_time.timestamp() * 1000),
            }

        return activity
