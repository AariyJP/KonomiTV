
import io
import json
import os
import socket
import struct
import sys
import uuid
from glob import glob
from typing import Any

import anyio

from app import logging


# IPC の読み書き中に発生しうる例外 (切断・JSON の破損・ヘッダーの不足)
## ConnectionError は OSError のサブクラスのため、OSError に含まれる
IPC_ERRORS = (OSError, ValueError, struct.error)


class DiscordRPCClient:
    """
    同じ PC で起動している Discord クライアントと IPC (Windows は名前付きパイプ、それ以外は UNIX ドメインソケット) で通信し、
    Rich Presence (アクティビティ) を設定するクライアント
    """

    def __init__(self, client_id: str) -> None:
        """
        DiscordRPCClient を初期化する

        Args:
            client_id (str): Rich Presence の送信元となる Discord アプリケーションの ID
        """

        # Rich Presence の送信元となる Discord アプリケーションの ID
        ## _connect() でのハンドシェイク時に参照される
        self.client_id = client_id

        # Discord クライアントとの IPC 接続
        ## Windows では名前付きパイプを開いた FileIO、それ以外では UNIX ドメインソケットから生成したファイルオブジェクトを保持し、
        ## どちらも read() / write() で同じように読み書きできるようにしている
        ## 未接続または切断済みの場合は None になる (is_connected / _send() / _read() から参照される)
        ## 各メソッドは DiscordRichPresenceTask の単一のタスクから1つずつ await して呼ばれる前提のため、排他制御は行わない
        self._connection: io.FileIO | io.BufferedRWPair | None = None


    @property
    def is_connected(self) -> bool:
        """
        Discord クライアントと接続中かどうか

        Returns:
            bool: 接続中なら True
        """

        return self._connection is not None


    async def connect(self) -> bool:
        """
        Discord クライアントに接続し、ハンドシェイクを行う
        IPC の読み書きはブロッキング I/O のため、ワーカースレッドで実行する

        Returns:
            bool: 接続に成功した (または既に接続済みの) 場合は True
        """

        return await anyio.to_thread.run_sync(self._connect)


    async def setActivity(self, activity: dict[str, Any] | None) -> bool:
        """
        Discord クライアントにアクティビティを設定する

        Args:
            activity (dict[str, Any] | None): 設定するアクティビティ (None を渡すとアクティビティを消去する)

        Returns:
            bool: 設定に成功した場合は True
        """

        return await anyio.to_thread.run_sync(self._setActivity, activity)


    async def close(self) -> None:
        """
        Discord クライアントとの接続を閉じる
        接続が閉じられると、Discord 側でこのプロセスが設定したアクティビティは自動的に消去される
        """

        await anyio.to_thread.run_sync(self._close)


    def _connect(self) -> bool:
        """
        Discord クライアントに接続し、ハンドシェイクを行う (ワーカースレッドで実行される)

        Returns:
            bool: 接続に成功した (または既に接続済みの) 場合は True
        """

        # 既に接続済みなら何もしない
        if self._connection is not None:
            return True

        # Discord クライアントは discord-ipc-0 から discord-ipc-9 のうち空いている番号で待ち受けているため、
        # 候補を順に試し、最初にハンドシェイクに成功したものを使う
        for ipc_path in self._getIPCPaths():
            try:
                # Windows では名前付きパイプをファイルとして開く
                if sys.platform == 'win32':
                    self._connection = io.FileIO(ipc_path, 'r+')
                # それ以外では UNIX ドメインソケットに接続し、読み書き用のファイルオブジェクトを生成する
                ## Discord クライアントが応答しなくなった場合に処理が止まり続けないよう、タイムアウトを設定しておく
                ## ソケットはファイルオブジェクトを生成した後に close() しても、ファイルオブジェクトを閉じるまでは接続が維持される
                else:
                    unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    unix_socket.settimeout(10)
                    unix_socket.connect(ipc_path)
                    self._connection = unix_socket.makefile('rwb')
                    unix_socket.close()

                # ハンドシェイク (opcode: 0) を送り、READY イベントが返ってくれば接続完了
                self._send(0, {'v': 1, 'client_id': self.client_id})
                if self._receive().get('evt') == 'READY':
                    logging.info(f'[DiscordRPCClient] Connected to Discord IPC. ({ipc_path})')
                    return True

            # 接続できなかった・Discord 側から切断された場合は次の候補を試す
            except IPC_ERRORS:
                pass
            self._close()

        return False


    def _setActivity(self, activity: dict[str, Any] | None) -> bool:
        """
        Discord クライアントにアクティビティを設定する (ワーカースレッドで実行される)

        Args:
            activity (dict[str, Any] | None): 設定するアクティビティ (None を渡すとアクティビティを消去する)

        Returns:
            bool: 設定に成功した場合は True
        """

        # 未接続なら何もしない
        if self._connection is None:
            return False

        # SET_ACTIVITY コマンドを送信し、レスポンスを受け取る
        ## pid には自プロセスの PID を渡す (このプロセスが終了すると Discord 側でアクティビティが消去される)
        try:
            self._send(1, {
                'cmd': 'SET_ACTIVITY',
                'args': {'pid': os.getpid(), 'activity': activity},
                'nonce': str(uuid.uuid4()),
            })
            response = self._receive()

        # Discord クライアントが終了したなどで切断された場合は、次回 connect() で再接続できるよう接続を破棄する
        except IPC_ERRORS:
            logging.warning('[DiscordRPCClient] Disconnected from Discord IPC.')
            self._close()
            return False

        # アクティビティの内容が不正などの理由で Discord 側がエラーを返した場合
        if response.get('evt') == 'ERROR':
            logging.warning(f'[DiscordRPCClient] Failed to set activity: {response.get("data")}')
            return False

        return True


    def _close(self) -> None:
        """
        IPC 接続を閉じて破棄する (ワーカースレッドで実行される)
        """

        if self._connection is not None:
            # 既に Discord 側から切断されている場合は close() で例外が発生することがあるが、破棄できれば問題ない
            try:
                self._connection.close()
            except OSError:
                pass
            self._connection = None


    def _getIPCPaths(self) -> list[str]:
        """
        Discord クライアントが待ち受けている可能性のある IPC のパスの候補を取得する

        Returns:
            list[str]: IPC のパスの候補のリスト
        """

        # Windows では名前付きパイプの番号違いのみ
        if sys.platform == 'win32':
            return [rf'\\.\pipe\discord-ipc-{index}' for index in range(10)]

        # Linux / macOS では、Discord クライアントのユーザーの一時ディレクトリにソケットが作成される
        ## KonomiTV が pm2 (root) で動作している場合、root の環境変数からはユーザーの一時ディレクトリが分からないため、
        ## /run/user/(UID) 以下も候補に含める
        base_dirs = [os.environ.get(key) for key in ('XDG_RUNTIME_DIR', 'TMPDIR', 'TMP', 'TEMP')]
        base_dirs += [*sorted(glob('/run/user/*')), '/tmp']

        # 重複を除いた各ディレクトリについて、通常版・Flatpak 版・Snap 版の Discord のソケットのパスを列挙する
        ipc_paths: list[str] = []
        for base_dir in dict.fromkeys(base_dir for base_dir in base_dirs if base_dir):
            for sub_dir in ('', 'app/com.discordapp.Discord', 'snap.discord'):
                for index in range(10):
                    ipc_path = os.path.join(base_dir, sub_dir, f'discord-ipc-{index}')
                    if os.path.exists(ipc_path):
                        ipc_paths.append(ipc_path)

        return ipc_paths


    def _send(self, opcode: int, payload: dict[str, Any]) -> None:
        """
        IPC にメッセージを送信する
        メッセージは opcode (4 バイト) ・ペイロード長 (4 バイト) ・JSON ペイロードの順に並べたリトルエンディアンのバイナリ

        Args:
            opcode (int): メッセージの種類 (0: ハンドシェイク, 1: フレーム, 2: 切断, 3: Ping, 4: Pong)
            payload (dict[str, Any]): 送信する JSON ペイロード

        Raises:
            ConnectionError: 未接続の場合
        """

        if self._connection is None:
            raise ConnectionError('Discord IPC is not connected.')
        data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self._connection.write(struct.pack('<II', opcode, len(data)) + data)
        self._connection.flush()


    def _receive(self) -> dict[str, Any]:
        """
        IPC からメッセージを1件受信する
        Ping には Pong を返して読み飛ばし、切断メッセージを受け取った場合は例外を送出する

        Returns:
            dict[str, Any]: 受信した JSON ペイロード

        Raises:
            ConnectionError: Discord 側から切断された場合
        """

        while True:
            opcode, length = struct.unpack('<II', self._read(8))
            payload = json.loads(self._read(length))
            # Ping (opcode: 3) には同じペイロードで Pong (opcode: 4) を返す
            if opcode == 3:
                self._send(4, payload)
                continue
            # 切断 (opcode: 2) を受け取った場合
            if opcode == 2:
                raise ConnectionError(payload.get('message'))
            return payload


    def _read(self, size: int) -> bytes:
        """
        IPC から指定されたバイト数を読み込む

        Args:
            size (int): 読み込むバイト数

        Returns:
            bytes: 読み込んだデータ

        Raises:
            ConnectionError: 未接続の場合、または読み込み途中で接続が閉じられた場合
        """

        if self._connection is None:
            raise ConnectionError('Discord IPC is not connected.')
        data = b''
        # 一度の読み込みで指定バイト数が揃うとは限らないため、揃うまで読み込みを繰り返す
        while len(data) < size:
            chunk = self._connection.read(size - len(data))
            # 空のデータが返ってきた場合は接続が閉じられている
            if not chunk:
                raise ConnectionError('Discord IPC connection closed.')
            data += chunk
        return data
