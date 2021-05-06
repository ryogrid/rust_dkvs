# coding:utf-8

import threading
from typing import Dict, List, TYPE_CHECKING
# from readerwriterlock import rwlock

if TYPE_CHECKING:
    from .chord_node import ChordNode
    from .node_info import NodeInfo
    from .chord_util import KeyValue
    from .chord_node import ChordNode

ID_SPACE_BITS = 30 # 160 <- sha1での本来の値
ID_SPACE_RANGE = 2**ID_SPACE_BITS # 0を含めての数である点に注意

# paramaters for executing by PyPy3 on my desktop machine
JOIN_INTERVAL_SEC = 1.0 #2.0 #0.9 #0.7 # 0.5 # 1
PUT_INTERVAL_SEC = 0.05 #0.5 # 0.01 #0.5 # 1
GET_INTERVAL_SEC = 0.05 #0.5 # 0.01 #0.5 # 1

# ノード増加の勢いは 係数-1/係数 となる
NODE_KILL_INTERVAL_SEC = 120.0 #20 #JOIN_INTERVAL_SEC * 10

# 全ノードがstabilize_successorを実行することを1バッチとした際に
# stabilize処理担当のスレッドにより呼び出されるstabilize処理を行わせる
# メソッドの一回の呼び出しで何バッチが実行されるか
STABILIZE_SUCCESSOR_BATCH_TIMES = 20 #10 #20
# 全ノードがstabilize_finger_tableを複数回呼びされることで、finger_tableの全要素を更新
# することを1バッチとした際に、stabilize処理担当のスレッドにより呼び出されるstabilize処理
# を行わせるメソッドの一回の呼び出しで何バッチが実行されるか
STABILIZE_FTABLE_BATCH_TIMES = 2 #1

# 一時的にこれより短くなる場合もある
SUCCESSOR_LIST_NORMAL_LEN = 3

# # 160bit符号なし整数の最大値
# # Chordネットワーク上のID空間の上限
# ID_MAX = 2**ID_SPACE_BITS - 1

# 30bit符号なし整数の最大値
# Chordネットワーク上のID空間の上限
# TODO: 検証時の実行時間短縮のためにハッシュ関数で求めた値の代わりに乱数
#       を用いているため bit数 を少なくしている
ID_MAX = ID_SPACE_RANGE - 1

KEEP_NODE_NUM = 50 #100
NODE_NUM_MAX = 10000

LOCK_ACQUIRE_TIMEOUT = 10 #3 #10

# プロセス内の全てのデータへのアクセスに対するロック変数
# 実装していく過程で細粒度のロックに対応できていない場合や、デバッグ用途に用いる
lock_of_all_data = threading.Lock()

# TODO: all_node_dictとall_data_listのロックはRustの該当するコレクションがスレッドセーフか
#       確認してから必要なところだけに絞る必要あり（例えば、readアクセスでも結果にセンシティブなところ以外は不要ではないかなど）

# アドレス文字列をキーとしてとり、対応するノードのChordNodeオブジェクトを返すハッシュ
# IPアドレスが分かれば、対応するノードと通信できることと対応している
all_node_dict : Dict[str, 'ChordNode'] = {}
lock_of_all_node_dict = threading.Lock()

# DHT上で保持されている全てのデータが保持されているリスト
# KeyValueオブジェクトを要素として持つ
# 全てのノードはputの際はDHTにデータをputするのとは別にこのリストにデータを追加し、
# getする際はDHTに対してgetを発行するためのデータをこのリストからランダム
# に選び、そのkeyを用いて探索を行う. また value も保持しておき、取得できた内容と
# 照らし合わせられるようにする
all_data_list : List['KeyValue'] = []
lock_of_all_data_list = threading.Lock()

# 検証を分かりやすくするために何ノード目として生成されたか
# のデバッグ用IDを持たせるためのカウンタ
already_born_node_num = 0

is_network_constructed = False

# デバッグ用の変数群
global_get_retry_cnt = 0
GLOBAL_GET_RETRY_CNT_LIMIT_TO_DEBEUG_PRINT = 30

# マスターデータとレプリカの区別なく、データIDをKeyに、当該IDに対応するデータを
# 保持しているノードのリストを得られる dict
all_data_placement_dict : Dict[str, List['NodeInfo']] = {}

# 既に発行したputの回数
already_issued_put_cnt = 0

# stabilize_successorのループの回せる回数の上限
TRYING_GET_SUCC_TIMES_LIMIT = SUCCESSOR_LIST_NORMAL_LEN * 5

# # デバッグ用のフラグ
# # killスレッドがWriter、他のスレッドはReaderとしてロックを取得する
# kill_thread_lock_factory = rwlock.RWLockFairD()
# kill_thread_write_lock = kill_thread_lock_factory.gen_wlock()
# kill_thread_read_lock = kill_thread_lock_factory.gen_rlock()

STABILIZE_THREAD_NUM = 10

ENABLE_DATA_STORE_OPERATION_DPRINT = False
ENABLE_ROUTING_INFO_DPRINT = False

# partial_join_opが実行されることを待っているノードが存在するか否か
# join と partial_join_op の間で、該当ノードがkillされることを避けるために用いる
is_waiting_partial_join_op_exists = False