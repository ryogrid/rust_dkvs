# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

import sys
import modules.gval as gval
import traceback
from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    InternalControlFlowException, DataIdAndValue, ErrorCode, PResult
from .taskqueue import TaskQueue

if TYPE_CHECKING:
    from .node_info import NodeInfo
    from .chord_node import ChordNode

class Stabilizer:

    # join が router.find_successorでの例外発生で失敗した場合にこのクラス変数に格納して次のjoin処理の際にリトライさせる
    # なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
    need_join_retry_node : Optional['ChordNode'] = None
    need_join_retry_tyukai_node: Optional['ChordNode'] = None

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node : 'ChordNode' = existing_node

    # 自ノードの持っている successor_info_listの deep copy を返す
    def pass_successor_list(self) -> List['NodeInfo']:
        # TODO: 実システム化する際はエンドポイントの頭なので必要なロックを全てとり、メソッドを抜ける際は全てreleaseするようなコードを書くこと
        return [ node_info.get_partial_deepcopy() for node_info in self.existing_node.node_info.successor_info_list]

    def pass_predecessor_info(self) -> Optional['NodeInfo']:
        # TODO: 実システム化する際はエンドポイントの頭なので必要なロックを全てとり、メソッドを抜ける際は全てreleaseするようなコードを書くこと
        if self.existing_node.node_info.predecessor_info != None:
            return cast('NodeInfo', self.existing_node.node_info.predecessor_info).get_partial_deepcopy()
        else:
            return None

    # successor_info_listの長さをチェックし、規定長を越えていた場合余剰なノードにレプリカを
    # 削除させた上で、リストから取り除く
    # TODO: InternalExp at check_successor_list_length
    def check_successor_list_length(self) -> PResult[bool]:
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("check_successor_list_length_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")
            return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

        try:
            ChordUtil.dprint(
                "check_successor_list_length_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + str(len(self.existing_node.node_info.successor_info_list)))

            if len(self.existing_node.node_info.successor_info_list) > gval.SUCCESSOR_LIST_NORMAL_LEN:
                list_len = len(self.existing_node.node_info.successor_info_list)
                delete_elem_list : List['NodeInfo'] = []
                for idx in range(gval.SUCCESSOR_LIST_NORMAL_LEN, list_len):
                    # successor_info_listからエントリが削除された場合、rangeで得られる数字列全てに要素がない
                    # 状態が起こるため、最新のlengthでチェックし直す
                    if idx >= len(self.existing_node.node_info.successor_info_list):
                        break

                    ChordUtil.dprint(
                        "check_successor_list_length_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                        + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[idx])
                        + str(len(self.existing_node.node_info.successor_info_list)))

                    delete_elem_list.append(self.existing_node.node_info.successor_info_list[idx])

                # 上のループで削除すると決まった要素を取り除く
                for elem in delete_elem_list:
                    self.existing_node.node_info.successor_info_list.remove(elem)

            return PResult.Ok(True)
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()

    # 経路表の情報を他ノードから強制的に設定する.
    # joinメソッドの中で、secondノードがfirstノードに対してのみ用いるものであり、他のケースで利用してはならない
    def set_routing_infos_force(self, predecessor_info : 'NodeInfo', successor_info_0 : 'NodeInfo', ftable_enry_0 : 'NodeInfo'):
        with self.existing_node.node_info.lock_of_pred_info, self.existing_node.node_info.lock_of_succ_infos:
            self.existing_node.node_info.predecessor_info = predecessor_info
            self.existing_node.node_info.successor_info_list[0] = successor_info_0
            self.existing_node.node_info.finger_table[0] = ftable_enry_0

    # node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
    def join(self, node_address : str):
        with self.existing_node.node_info.lock_of_pred_info, self.existing_node.node_info.lock_of_succ_infos, self.existing_node.node_info.lock_of_datastore:
            # 実装上例外は発生しない.
            # また実システムでもダウンしているノードの情報が与えられることは想定しない
            #tyukai_node = ChordUtil.get_node_by_address(node_address)
            tyukai_node = cast('ChordNode', ChordUtil.get_node_by_address(node_address).result)
            # TODO: x direct access to node_info of tyukai_node at join
            ChordUtil.dprint("join_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))

            # try:

            # 仲介ノードに自身のsuccessorになるべきノードを探してもらう

            # TODO: find_successor call at join
            #successor = tyukai_node.endpoints.grpc__find_successor(self.existing_node.node_info.node_id)
            ret = tyukai_node.endpoints.grpc__find_successor(self.existing_node.node_info.node_id)
            if (ret.is_ok):
                successor : 'ChordNode' = cast('ChordNode', ret.result)
                # リトライは不要なので、本メソッドの呼び出し元がリトライ処理を行うかの判断に用いる
                # フィールドをリセットしておく
                Stabilizer.need_join_retry_node = None
            else:  # ret.err_code == ErrorCode.AppropriateNodeNotFoundException_CODE || ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                # リトライに必要な情報を記録しておく
                Stabilizer.need_join_retry_node = self.existing_node
                Stabilizer.need_join_retry_tyukai_node = tyukai_node

                # 自ノードの情報、仲介ノードの情報
                # TODO: x direct access to node_info of tyukai_node at join
                ChordUtil.dprint(
                    "join_2,RETRY_IS_NEEDED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))
                return

            # except (AppropriateNodeNotFoundException, NodeIsDownedExceptiopn, InternalControlFlowException):
            #     # リトライに必要な情報を記録しておく
            #     Stabilizer.need_join_retry_node = self.existing_node
            #     Stabilizer.need_join_retry_tyukai_node = tyukai_node
            #
            #     # 自ノードの情報、仲介ノードの情報
            #     # TODO: x direct access to node_info of tyukai_node at join
            #     ChordUtil.dprint("join_2,RETRY_IS_NEEDED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            #                      + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))
            #     return

            # try:

            # TODO: x direct access to node_info of successor at join
            self.existing_node.node_info.successor_info_list.append(successor.node_info.get_partial_deepcopy())

            # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
            self.existing_node.node_info.finger_table[0] = self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()

            # TODO: x direct access to node_info of tyukai_node at join
            if tyukai_node.node_info.node_id == tyukai_node.node_info.successor_info_list[0].node_id:
                # secondノードの場合の考慮 (仲介ノードは必ずfirst node)

                predecessor = tyukai_node

                # 2ノードでsuccessorでもpredecessorでも、チェーン構造で正しい環が構成されるよう強制的に全て設定してしまう
                # TODO: x direct access to node_info of predecessor at join
                self.existing_node.node_info.predecessor_info = predecessor.node_info.get_partial_deepcopy()

                tyukai_node.endpoints.grpc__set_routing_infos_force(
                    self.existing_node.node_info.get_partial_deepcopy(),
                    self.existing_node.node_info.get_partial_deepcopy(),
                    self.existing_node.node_info.get_partial_deepcopy()
                )

                # tyukai_node.node_info.predecessor_info = self.existing_node.node_info.get_partial_deepcopy()
                # tyukai_node.node_info.successor_info_list[0] = self.existing_node.node_info.get_partial_deepcopy()
                # # fingerテーブルの0番エントリも強制的に設定する
                # tyukai_node.node_info.finger_table[0] = self.existing_node.node_info.get_partial_deepcopy()

                # TODO: x direct access to node_info of tyukai_node at join
                ChordUtil.dprint("join_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))
            else:
                # successorと、successorノードの情報だけ適切なものとする
                # TODO: check_predecessor call at join
                    #successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
                ret2 = successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
                if (ret2.is_ok):
                    pass
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    # リトライに必要な情報を記録しておく
                    Stabilizer.need_join_retry_node = self.existing_node
                    Stabilizer.need_join_retry_tyukai_node = tyukai_node

                    # 既に値を設定してしまっている場合を考慮し、内容をリセットしておく
                    self.existing_node.node_info.successor_info_list = []

                    # 自ノードの情報、仲介ノードの情報
                    # TODO: x direct access to node_info of tyukai_node at join
                    ChordUtil.dprint("join_3,RETRY_IS_NEEDED," + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))
                    ChordUtil.dprint(traceback.format_exc())
                    return PResult.Err(False, cast(int, ret2.err_code))

                # successor_info_listを埋めておく
                # TODO: pass_successor_list call at join
                succ_list_of_succ: List[NodeInfo] = successor.endpoints.grpc__pass_successor_list()
                list_len = len(succ_list_of_succ)
                for idx in range(0, gval.SUCCESSOR_LIST_NORMAL_LEN - 1):
                    if idx < list_len:
                        self.existing_node.node_info.successor_info_list.append(
                            succ_list_of_succ[idx].get_partial_deepcopy())

            # successorから自身が担当することになるID範囲のデータの委譲を受け、格納する

            # TODO: delegate_my_tantou_data call at join
            tantou_data_list: List[KeyValue] = successor.endpoints.grpc__delegate_my_tantou_data(
                self.existing_node.node_info.node_id)

            #with self.existing_node.node_info.lock_of_datastore:
            for key_value in tantou_data_list:
                self.existing_node.data_store.store_new_data(cast(int, key_value.data_id), key_value.value_data)

            # 残りのレプリカに関する処理は stabilize処理のためのスレッドに別途実行させる
            self.existing_node.tqueue.append_task(TaskQueue.JOIN_PARTIAL)
            gval.is_waiting_partial_join_op_exists = True

            ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

            # except (InternalControlFlowException, NodeIsDownedExceptiopn):
            #     # リトライに必要な情報を記録しておく
            #     Stabilizer.need_join_retry_node = self.existing_node
            #     Stabilizer.need_join_retry_tyukai_node = tyukai_node
            #
            #     # 既に値を設定してしまっている場合を考慮し、内容をリセットしておく
            #     self.existing_node.node_info.successor_info_list = []
            #
            #     # 自ノードの情報、仲介ノードの情報
            #     # TODO: x direct access to node_info of tyukai_node at join
            #     ChordUtil.dprint("join_3,RETRY_IS_NEEDED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            #                      + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))
            #     ChordUtil.dprint(traceback.format_exc())
            #     return

    # join処理のうちレプリカに関する処理を分割したもの
    # stabilize処理を行うスレッドによって一度だけ(失敗した場合はカウントしないとして)実行される
    # TODO: InternalExp at partial_join_op
    def partial_join_op(self) -> PResult[bool]:
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint(
                "partial_join_op_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
            return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint(
                "partial_join_op_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")
            return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
        if self.existing_node.node_info.lock_of_datastore.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            self.existing_node.node_info.lock_of_succ_infos.release()
            ChordUtil.dprint(
                "partial_join_op_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")
            return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

        if self.existing_node.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            self.existing_node.node_info.lock_of_datastore.release()
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint("partial_join_op_2_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            return PResult.Ok(True)

        ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

        try:
            # successor[0] から委譲を受けたデータを successorList 内の全ノードにレプリカとして配る
            #tantou_data_list : List[DataIdAndValue] = self.existing_node.data_store.get_all_tantou_data()
            ret0 = self.existing_node.data_store.get_all_tantou_data()
            if (ret0.is_ok):
                tantou_data_list: List[DataIdAndValue] = cast(List[DataIdAndValue], ret0.result)
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                # データの委譲自体は既に受けているため、リトライされるようにする
                return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

            for node_info in self.existing_node.node_info.successor_info_list:
                # try:
                    #succ : 'ChordNode' = ChordUtil.get_node_by_address(node_info.address_str)
                ret = ChordUtil.get_node_by_address(node_info.address_str)
                if (ret.is_ok):
                    succ: 'ChordNode' = cast('ChordNode', ret.result)
                    ChordUtil.dprint("partial_join_op_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(node_info) + "," + str(len(self.existing_node.node_info.successor_info_list)))

                    # TODO: receive_replica call at partial_join_op
                    succ.endpoints.grpc__receive_replica(
                        [DataIdAndValue(data_id = data.data_id, value_data=data.value_data) for data in tantou_data_list]
                    )
                else:  # ret1.err_code == ErrorCode.InternalControlFlowException_CODE || ret1.err_code == ErrorCode.NodeIsDownedException_CODE
                    # ノードがダウンしていた場合等は無視して次のノードに進む.
                    # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                    # put処理 の中で後ほど行われるためここでは対処しない
                    # (ただし、レプリカが当該ノードに存在しない状態が短くない時間発生する可能性はある)
                    ChordUtil.dprint("partial_join_op_4,NODE_IS_DOWNED or InternalControlFlowException,"
                                     + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(node_info))
                    continue

                # except (NodeIsDownedExceptiopn, InternalControlFlowException):
                #     # ノードがダウンしていた場合等は無視して次のノードに進む.
                #     # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                #     # put処理 の中で後ほど行われるためここでは対処しない
                #     # (ただし、レプリカが当該ノードに存在しない状態が短くない時間発生する可能性はある)
                #     ChordUtil.dprint("partial_join_op_4,NODE_IS_DOWNED or InternalControlFlowException,"
                #                      + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                #                      + ChordUtil.gen_debug_str_of_node(node_info))
                #     continue

            def handle_err():
                ChordUtil.dprint(
                    "partial_join_op_6,NODE_IS_DOWNED or InternalControlFlowException" + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(node_info))
                # ノードがダウンしていた場合等は無視して先に進む.
                # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                # put処理 の中で後ほど行われるためここでは対処しない
                # (ただし、レプリカが本ノードに存在しない状態が短くない時間発生する可能性はある)
                pass

            if self.existing_node.node_info.predecessor_info != None:
                # predecessorが非Noneであれば当該ノードの担当データをレプリカとして保持しておかなければならないため
                # データを渡してもらい、格納する
                self_predecessor_info : NodeInfo = cast('NodeInfo', self.existing_node.node_info.predecessor_info)
                # try:
                    #self_predeessor_node : 'ChordNode' = ChordUtil.get_node_by_address(self_predecessor_info.address_str)
                ret1 = ChordUtil.get_node_by_address(self_predecessor_info.address_str)
                if (ret1.is_ok):
                    self_predeessor_node: 'ChordNode' = cast('ChordNode', ret1.result)
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                    handle_err()
                    return PResult.Ok(True)

                # TODO: get_all_tantou_data call at partial_join_op
                #pred_tantou_datas : List[DataIdAndValue] = self_predeessor_node.endpoints.grpc__get_all_tantou_data()
                ret = self_predeessor_node.endpoints.grpc__get_all_tantou_data()
                if (ret.is_ok):
                    pred_tantou_datas : List[DataIdAndValue] = cast(List[DataIdAndValue], ret.result)
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    pred_tantou_datas : List[DataIdAndValue] = []

                for iv_entry in pred_tantou_datas:
                    self.existing_node.data_store.store_new_data(iv_entry.data_id,
                                                                 iv_entry.value_data,
                                                                 )

                ChordUtil.dprint("partial_join_op_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self_predeessor_node.node_info) + "," + str(len(pred_tantou_datas)))

                # predecessor が非Noneであれば、当該predecessorのsuccessor_info_listの長さが標準を越えてしまって
                # いる場合があるため、そのチェックを行う
                # (この呼び出しの中で successor_info_listからの余剰ノードのエントリ削除も行われる）
                # TODO: check_successor_list_length call at partial_join_op
                #self_predeessor_node.endpoints.grpc__check_successor_list_length()
                ret3 = self_predeessor_node.endpoints.grpc__check_successor_list_length()
                if (ret3.is_ok):
                    pass
                else:  # ret3.err_code == ErrorCode.InternalControlFlowException_CODE || ret3.err_code == ErrorCode.NodeIsDownedException_CODE
                    handle_err()
                    return PResult.Ok(True)

                # except (NodeIsDownedExceptiopn, InternalControlFlowException):
                #     ChordUtil.dprint("partial_join_op_6,NODE_IS_DOWNED or InternalControlFlowException" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                #                      + ChordUtil.gen_debug_str_of_node(node_info))
                #     # ノードがダウンしていた場合等は無視して先に進む.
                #     # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                #     # put処理 の中で後ほど行われるためここでは対処しない
                #     # (ただし、レプリカが本ノードに存在しない状態が短くない時間発生する可能性はある)
                #     pass

            # successorから保持している全てのレプリカを受け取り格納する（successorよりは前に位置することになるため、
            # 基本的にsuccessorが保持しているレプリカは自身も全て保持している状態とならなければならない）
            # (前方に位置するノードが join や put によるレプリカの配布を行っているタイミングとバッティングするとsuccessorが持っている古い
            #  データで更新してしまうということが起こる可能性はあるが、タイミングとしては稀であり、また後続の put で再度最新のデータを受け取る
            #  ため、問題ないと判断する)
            # try:

            # successor : 'ChordNode' = ChordUtil.get_node_by_address(self.existing_node.node_info.successor_info_list[0].address_str)
            ret4 = ChordUtil.get_node_by_address(self.existing_node.node_info.successor_info_list[0].address_str)
            if (ret4.is_ok):
                successor : 'ChordNode' = cast('ChordNode', ret4.result)
                # TODO: get_all_data call at partial_join_op
                passed_all_replica: List[DataIdAndValue] = successor.endpoints.grpc__get_all_data()
                self.existing_node.data_store.store_replica_of_multi_masters(passed_all_replica)
            else:  # ret4.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                # ノードがダウンしていた場合等は無視して先に進む.
                # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                # put処理 の中で後ほど行われるためここでは対処しない
                # (ただし、レプリカが本ノードに存在しない状態が短くない時間発生する可能性はある)
                ChordUtil.dprint(
                    "partial_join_op_7,NODE_IS_DOWNED or InternalControlFlowException" + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(node_info))

            # except (NodeIsDownedExceptiopn, InternalControlFlowException):
            #     # ノードがダウンしていた場合等は無視して先に進む.
            #     # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
            #     # put処理 の中で後ほど行われるためここでは対処しない
            #     # (ただし、レプリカが本ノードに存在しない状態が短くない時間発生する可能性はある)
            #     ChordUtil.dprint(
            #         "partial_join_op_7,NODE_IS_DOWNED or InternalControlFlowException" + ChordUtil.gen_debug_str_of_node(
            #             self.existing_node.node_info) + ","
            #         + ChordUtil.gen_debug_str_of_node(node_info))

            # join処理が全て終わった
            self.existing_node.is_join_op_finished = True
            # partial_join_opが終わるまで止めていたkillスレッドを解放する
            gval.is_waiting_partial_join_op_exists = False

            # 自ノードの情報、仲介ノードの情報、successorとして設定したノードの情報
            ChordUtil.dprint("partial_join_op_8," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            return PResult.Ok(True)
        finally:
            self.existing_node.node_info.lock_of_datastore.release()
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()

    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
    # Attention: InternalControlFlowException を raiseする場合がある
    # TODO: InternalExp at check_predecessor
    def check_predecessor(self, node_info : 'NodeInfo') -> PResult[bool]:
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("check_predecessor_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
            return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

        ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

        try:
            if self.existing_node.node_info.predecessor_info == None:
                # predecesorが設定されていなければ無条件にチェックを求められたノードを設定する
                self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()
                ChordUtil.dprint("check_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            ChordUtil.dprint("check_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            # この時点で認識している predecessor がノードダウンしていないかチェックする
            # is_pred_alived = ChordUtil.is_node_alive(cast('NodeInfo', self.existing_node.node_info.predecessor_info).address_str)
            ret = ChordUtil.is_node_alive(cast('NodeInfo', self.existing_node.node_info.predecessor_info).address_str)
            if (ret.is_ok):
                is_pred_alived : bool = cast(bool, ret.result)
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                is_pred_alived : bool = False

            if is_pred_alived:
                distance_check = ChordUtil.calc_distance_between_nodes_left_mawari(self.existing_node.node_info.node_id, node_info.node_id)
                distance_cur = ChordUtil.calc_distance_between_nodes_left_mawari(self.existing_node.node_info.node_id,
                                                                                 cast('NodeInfo',self.existing_node.node_info.predecessor_info).node_id)

                # 確認を求められたノードの方が現在の predecessor より predecessorらしければ
                # 経路表の情報を更新する
                if distance_check < distance_cur:
                    self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()

                    ChordUtil.dprint("check_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                          + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                          + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.predecessor_info))
            else: # predecessorがダウンしていた場合は無条件でチェックを求められたノードをpredecessorに設定する
                self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()

            return PResult.Ok(True)
        finally:
            self.existing_node.node_info.lock_of_pred_info.release()

    # ロックは呼び出し元のstabilize_successor_innerでとってある前提
    # TODO: DownedExp at stabilize_successor_inner_fill_succ_list
    def stabilize_successor_inner_fill_succ_list(self) -> PResult[bool]:
        # 本メソッド呼び出しでsuccessorとして扱うノードはsuccessorListからダウンしているノードを取り除いた上で
        # successor_info_list[0]となったノードとする
        ChordUtil.dprint("stabilize_successor_inner_fill_succ_list_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        if self.existing_node.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            ChordUtil.dprint(
                "stabilize_successor_innner_fill_succ_list_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            #raise NodeIsDownedExceptiopn()
            return PResult.Err(False, ErrorCode.NodeIsDownedException_CODE)

        successor_list_tmp: List['NodeInfo'] = []
        for idx in range(len(self.existing_node.node_info.successor_info_list)):
            # try:
                #if ChordUtil.is_node_alive(self.existing_node.node_info.successor_info_list[idx].address_str):
            ret = ChordUtil.is_node_alive(self.existing_node.node_info.successor_info_list[idx].address_str)
            if (ret.is_ok):
                if cast(bool, ret.result) == True:
                    successor_list_tmp.append(self.existing_node.node_info.successor_info_list[idx])
                else: # == False
                    ChordUtil.dprint("stabilize_successor_inner_fill_succ_list_1,SUCCESSOR_IS_DOWNED,"
                                     + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info.successor_info_list[idx]))
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                # した際に発生してしまうので、ここで対処する
                # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなので、後続の処理を行わず successor[0]を返す
                # TODO: なお、join処理中のノードがsuccessor_info_listに入っていた場合も同様に例外がraiseされるケースがある
                continue

            # except InternalControlFlowException:
            #     # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
            #     # した際に発生してしまうので、ここで対処する
            #     # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなので、後続の処理を行わず successor[0]を返す
            #     # TODO: なお、join処理中のノードがsuccessor_info_listに入っていた場合も同様に例外がraiseされるケースがある
            #     continue

        if len(successor_list_tmp) == 0:
            # successorListの全てのノードを当たっても、生きているノードが存在しなかった場合
            # 起きてはいけない状況なので例外を投げてプログラムを終了させる
            ChordUtil.dprint("stabilize_successor_inner_fill_succ_list_3,,"
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + str(len(self.existing_node.node_info.successor_info_list)) + ","
                             + str(self.existing_node.node_info.successor_info_list),
                             flush=True)
            raise Exception("Maybe some parameters related to fault-tolerance of Chord network are not appropriate")
        else:
            self.existing_node.node_info.successor_info_list = successor_list_tmp

        return PResult.Ok(True)

    # TODO: InternalExp, NodeIsDownedExp at stabilize_successor_inner_fix_chain
    # ロックは呼び出し元のstabilize_successor_innerでとってある前提
    def stabilize_successor_inner_fix_chain(self, successor : 'ChordNode') -> PResult[bool]:
        # TODO: direct access to predecessor_info of successor at stabilize_successor_inner_fix_chain

        # predecessor_infoが未設定の場合の考慮
        if successor.node_info.predecessor_info == None:
            pred_id_of_successor = -1
            # 以下の呼び出しにより、successorの predecessor_info には自ノードが設定される
            successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
        else:
            pred_id_of_successor = cast('NodeInfo', successor.node_info.predecessor_info).node_id

        ChordUtil.dprint(
            "stabilize_successor_inner_fix_chain_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
            + str(pred_id_of_successor))

        # predecessor_infoが未設定であったが、grpc_check_predecessorによって設定された場合のため再度
        # pred_id_of_successorを取得する
        # なお、後続の処理を進める意味は無いが、ひとまずそのまま進める
        pred_id_of_successor = cast('NodeInfo', successor.node_info.predecessor_info).node_id

        if self.existing_node.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            ChordUtil.dprint(
                "stabilize_successor_innner_fix_chain_1_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            #raise NodeIsDownedExceptiopn()
            return PResult.Err(False, ErrorCode.NodeIsDownedException_CODE)

        # successorが認識している predecessor の情報をチェックさせて、適切なものに変更させたり、把握していない
        # 自身のsuccessor[0]になるべきノードの存在が判明した場合は 自身の successor[0] をそちらに張り替える.
        # なお、下のパターン1から3という記述は以下の資料による説明に基づく
        # https://www.slideshare.net/did2/chorddht
        if (pred_id_of_successor == self.existing_node.node_info.node_id):
            # パターン1
            # 特に訂正は不要
            ChordUtil.dprint(
                "stabilize_successor_inner_fix_chain_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                + str(pred_id_of_successor))
        else:
            # 以下、パターン2およびパターン3に対応する処理

            # try:

            # 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
            # 情報を更新してもらう
            # 注: successorが認識していた predecessorがダウンしていた場合、下の呼び出しにより後続でcheck_predecessorを
            #     を呼び出すまでもなく、successorのpredecessorは自身になっている. 従って後続でノードダウン検出した場合の
            #     check_predecessorの呼び出しは不要であるが呼び出しは行うようにしておく

                #successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
            ret = successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
            if (ret.is_ok):
                pass
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                # した際にcheck_predecessorで発生する場合があるので、ここで対処する
                # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなの特に処理は不要
                ChordUtil.dprint(
                    "stabilize_successor_inner_fix_chain_3," + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            # except (InternalControlFlowException, NodeIsDownedExceptiopn):
            #     # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
            #     # した際にcheck_predecessorで発生する場合があるので、ここで対処する
            #     # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなの特に処理は不要
            #     ChordUtil.dprint(
            #         "stabilize_successor_inner_fix_chain_3," + ChordUtil.gen_debug_str_of_node(
            #             self.existing_node.node_info) + ","
            #         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            # TODO: x direct access to node_info of successor at stabilize_successor_inner_fix_chain
            distance_unknown = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id,
                                                                                 pred_id_of_successor)
            # TODO: x direct access to node_info of successor at stabilize_successor_inner_fix_chain
            distance_me = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id,
                                                                            self.existing_node.node_info.node_id)
            if distance_unknown < distance_me:
                # successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
                # successorから自身に対して前方向にたどった場合の経路中に存在する場合
                # 自身の認識するsuccessorの情報を更新する

                def handle_err() -> PResult[bool]:
                    # 例外発生時は張り替えを中止する
                    #   - successorは変更しない
                    #   - この時点でのsuccessor[0]が認識するpredecessorを自身とする(successr[0]のcheck_predecessorを呼び出す)

                    # successor[0]の変更は行わず、ダウンしていたノードではなく自身をpredecessorとするよう(間接的に)要請する
                    # TODO: check_predecessor call at stabilize_successor_inner_fix_chain
                    #successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
                    ret2 = successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
                    if (ret2.is_ok):
                        pass
                    else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                        ChordUtil.dprint("stabilize_successor_inner_fix_chain_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                            + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))
                        return PResult.Err(False, cast(int, ret2.err_code))

                    ChordUtil.dprint("stabilize_successor_inner_fix_chain_5," + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info.successor_info_list[0]))

                    return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

                # try:

                # TODO: x direct access to predecessor_info of successor at stabilize_successor_inner_fix_chain
                # new_successor = ChordUtil.get_node_by_address(
                #     cast('NodeInfo', successor.node_info.predecessor_info).address_str)
                ret3 = ChordUtil.get_node_by_address(cast('NodeInfo', successor.node_info.predecessor_info).address_str)
                if (ret3.is_ok):
                    new_successor: 'ChordNode' = cast('ChordNode', ret3.result)
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    return handle_err()

                # TODO: x direct access to node_info of new_successor at stabilize_successor_inner_fix_chain
                self.existing_node.node_info.successor_info_list.insert(0,
                                                                        new_successor.node_info.get_partial_deepcopy())

                # 新たなsuccesorに対して担当データのレプリカを渡す
                #tantou_data_list: List[DataIdAndValue] = self.existing_node.data_store.get_all_tantou_data()
                ret4 = self.existing_node.data_store.get_all_tantou_data()
                if (ret4.is_ok):
                    tantou_data_list: List[DataIdAndValue] = cast(List[DataIdAndValue], ret4.result)
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    # putされた時にいずれ受け取るので良いことにする
                    tantou_data_list: List[DataIdAndValue] = []

                # TODO: receive_replica call at stabilize_successor_inner_fix_chain
                new_successor.endpoints.grpc__receive_replica(tantou_data_list)

                # successorListから溢れたノードがいた場合、自ノードの担当データのレプリカを削除させ、successorListから取り除く
                # (この呼び出しの中でsuccessorListからのノード情報の削除も行われる)

                #self.check_successor_list_length()
                ret5 = self.check_successor_list_length()
                if (ret5.is_ok):
                    pass
                else:  # ret5.err_code == ErrorCode.InternalControlFlowException_CODE
                    return handle_err()

                # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
                # ば情報を更新してもらう
                # TODO: check_predecessor call at stabilize_successor_inner_fix_chain
                #new_successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
                ret6 = new_successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
                if (ret6.is_ok):
                    pass
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    return handle_err()

                # TODO: x direct access to node_info of new_successor at stabilize_successor_inner_fix_chain
                ChordUtil.dprint("stabilize_successor_inner_fix_chain_3," + ChordUtil.gen_debug_str_of_node(
                    self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(
                    self.existing_node.node_info.successor_info_list[0]) + ","
                                 + ChordUtil.gen_debug_str_of_node(new_successor.node_info))

                # except (InternalControlFlowException, NodeIsDownedExceptiopn):
                #     # 例外発生時は張り替えを中止する
                #     #   - successorは変更しない
                #     #   - この時点でのsuccessor[0]が認識するpredecessorを自身とする(successr[0]のcheck_predecessorを呼び出す)
                #
                #
                #     # successor[0]の変更は行わず、ダウンしていたノードではなく自身をpredecessorとするよう(間接的に)要請する
                #     # TODO: check_predecessor call at stabilize_successor_inner_fix_chain
                #     successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
                #     ChordUtil.dprint("stabilize_successor_inner_fix_chain_4," + ChordUtil.gen_debug_str_of_node(
                #         self.existing_node.node_info) + ","
                #                      + ChordUtil.gen_debug_str_of_node(
                #         self.existing_node.node_info.successor_info_list[0]))

        return PResult.Ok(True)



    #  ノードダウンしておらず、チェーンの接続関係が正常 (predecessorの情報が適切でそのノードが生きている)
    #  なノードで、諸々の処理の結果、self の successor[0] となるべきノードであると確認されたノードを返す.
    #　注: この呼び出しにより、self.existing_node.node_info.successor_info_list[0] は更新される
    #  規約: 呼び出し元は、selfが生きていることを確認した上で本メソッドを呼び出さなければならない
    # TODO: InternalExp, DownedExp at stabilize_successor_inner
    def stabilize_successor_inner(self) -> PResult[Optional['NodeInfo']]:
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("stabilize_successor_inner_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint("find_successor_inner_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")
            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

        if self.existing_node.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint(
                "stabilize_successor_innner_0_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            #raise InternalControlFlowException("request received but I am already dead.")
            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

        ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

        try:
            # self.existing_node.node_info.successor_info_listを生きているノードだけにする

            #self.stabilize_successor_inner_fill_succ_list()
            ret = self.stabilize_successor_inner_fill_succ_list()
            if (ret.is_ok):
                pass
            else:  # ret.err_code == ErrorCode.NodeIsDownedException_CODE
                return PResult.Err(None, cast(int, ret.err_code))

            succ_addr = self.existing_node.node_info.successor_info_list[0].address_str

            #if not ChordUtil.is_node_alive(succ_addr):
            ret2 = ChordUtil.is_node_alive(succ_addr)
            if (ret2.is_ok):
                if cast(bool, ret2.result) == False:
                    # raise InternalControlFlowException("successor[0] is downed.")
                    return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                return PResult.Err(None, cast(int, ret2.err_code))


            # TODO: ひとまずここでの get_node_by_address の呼び出しはエラーを変えさない前提とする at stabilize_successor_inner
            successor = cast('ChordNode', ChordUtil.get_node_by_address(succ_addr).result)

            #self.stabilize_successor_inner_fix_chain(successor)
            ret3 = self.stabilize_successor_inner_fix_chain(successor)
            if (ret3.is_ok):
                pass
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                return PResult.Err(None, cast(int, ret3.err_code))

            return PResult.Ok(self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy())
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()

    # successorListに関するstabilize処理を行う
    # コメントにおいては、successorListの構造を意識した記述の場合、一番近いsuccessorを successor[0] と
    # 記述し、以降に位置するノードは近い順に successor[idx] と記述する
    # TODO: InternalExp at stabilize_successor
    def stabilize_successor(self) -> PResult[bool]:
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("stabilize_successor_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
            return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint("stabilize_successor_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")
            return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

        if self.existing_node.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint("stabilize_successor_0_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            return PResult.Ok(True)

        # with self.existing_node.node_info.lock_of_datastore:
        #     # stabilizeの度に、担当データとして保持しているデータ全てのレプリカを successor_info_list 内のノードに
        #     # 配布する
        #     self.existing_node.data_store.distribute_replica()

        try:
            ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            # 後続のノード（successorや、successorのsuccessor ....）を辿っていき、
            # downしているノードをよけつつ、各ノードの接続関係を正常に修復していきつつ、
            # self.existing_node.node_info.successor_info_list に最大で gval.SUCCESSOR_LIST_NORMAL_LEN個
            # のノード情報を詰める.
            # 処理としては successor 情報を1ノード分しか保持しない設計であった際のstabilize_successorを
            # successorList内のノードに順に呼び出して、stabilize処理を行わせると同時に、そのノードのsuccessor[0]
            # を返答させるといったものである.

            # 最終的に self.existing_node.node_info.successor_info_listに上書きするリスト
            updated_list : List['NodeInfo'] = []

            # 最初は自ノードを指定してそのsuccessor[0]を取得するところからスタートする
            cur_node : 'ChordNode' = self.existing_node
            last_node_info : 'NodeInfo' = self.existing_node.node_info

            tried_getting_succ_cnt = 0
            exception_occured = False
            cur_backup_succ_list = self.existing_node.node_info.successor_info_list
            # 正常に、もしくは正常な、successor_info_listに入れるべきノードが取得できなかった
            # 場合に、バックアップとして利用する cur_backup_succ_listの参照すべきインデックス
            cur_backup_node_info_idx = 0
            while len(updated_list) < gval.SUCCESSOR_LIST_NORMAL_LEN and tried_getting_succ_cnt < gval.TRYING_GET_SUCC_TIMES_LIMIT:
                try:
                    if exception_occured == False:
                        # TODO: stabilize_successor_inner call at stabilize_successor
                        #cur_node_info : 'NodeInfo' = cur_node.endpoints.grpc__stabilize_successor_inner()
                        ret = cur_node.endpoints.grpc__stabilize_successor_inner()
                        if (ret.is_ok):
                            cur_node_info : 'NodeInfo' = cast('NodeInfo', ret.result)
                        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                            # cur_nodeがjoin中のノードでget_node_by_addressで例外が発生してしまったか、
                            # ロックの取得でタイムアウトが発生した
                            # あるいは、cur_node(selfの場合もあれば他ノードの場合もある)が、生存している状態が通常期待される
                            # ところで、node_kill_th の処理がノードをダウン状態にしてしまった
                            ChordUtil.dprint(
                                "stabilize_successor_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info)
                                + ",TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                            exception_occured = True
                            continue
                    else:
                        cur_node_info : 'NodeInfo' = last_node_info

                    if cur_node_info.node_id == self.existing_node.node_info.node_id or exception_occured == True:
                        # 返ってきたノード情報は無視し、元々持っている node_info_list内のノードを返ってきたノード情報として扱う
                        # 長さが足りなかった場合はあきらめる
                        if cur_backup_node_info_idx + 1 < len(cur_backup_succ_list):
                            ChordUtil.dprint("stabilize_successor_2,RETURNED_NODE_SAME_AS_SELF," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                             + ChordUtil.gen_debug_str_of_node(cur_node_info))
                            # 返ってきたノードが適切なものでなかったという意味で、正常にループが回った場合や、例外処理が行われた
                            # 場合のインクリメントに加えて、インクリメントを行っておかなくてはならない
                            cur_backup_node_info_idx += 1
                            cur_node_info = cur_backup_succ_list[cur_backup_node_info_idx].get_partial_deepcopy()
                        else:
                            ChordUtil.dprint("stabilize_successor_2_5,RETURNED_NODE_SAME_AS_SELF_AND_END_SEARCH_SUCCESSOR," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                             + ChordUtil.gen_debug_str_of_node(cur_node_info))
                            break

                    ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(cur_node_info) + ","
                                     + str(exception_occured))

                    #cur_node : 'ChordNode' = ChordUtil.get_node_by_address(cur_node_info.address_str)
                    ret2 = ChordUtil.get_node_by_address(cur_node_info.address_str)
                    if (ret2.is_ok):
                        cur_node: 'ChordNode' = cast('ChordNode', ret2.result)
                    else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                        # cur_nodeがjoin中のノードでget_node_by_addressで例外が発生してしまったか、
                        # ロックの取得でタイムアウトが発生した
                        # あるいは、cur_node(selfの場合もあれば他ノードの場合もある)が、生存している状態が通常期待される
                        # ところで、node_kill_th の処理がノードをダウン状態にしてしまった
                        ChordUtil.dprint(
                            "stabilize_successor_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info)
                            + ",TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                        exception_occured = True
                        continue

                    # cur_nodeが取得できたということは、cur_nodeはダウンしていない
                    # チェーンを辿っている中で、生存しているノードが得られた場合は、辿っていく中で
                    # 例外が発生した際などに、チェーンを辿らずにノード情報を得るために用いる successorのリスト
                    # をそちらに置き換える
                    # TODO: pass_successor_list call at stabilize_successor
                    cur_backup_succ_list = cur_node.endpoints.grpc__pass_successor_list()
                    # 利用するリストが置き換わったので、それに合わせてインデックスをリセットする
                    # finally節で +1 するので筋悪ではあるが、-1にしておく
                    cur_backup_node_info_idx = -1

                    ChordUtil.dprint(
                        "stabilize_successor_3_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                        + ChordUtil.gen_debug_str_of_node(cur_node_info) + ","
                        + str(cur_backup_succ_list) + ","
                        + str(cur_backup_node_info_idx) + ","
                        + str(exception_occured))

                    updated_list.append(cur_node_info)
                    exception_occured = False
                    last_node_info = cur_node_info
                # except (InternalControlFlowException, NodeIsDownedExceptiopn):
                #     # cur_nodeがjoin中のノードでget_node_by_addressで例外が発生してしまったか、
                #     # ロックの取得でタイムアウトが発生した
                #     # あるいは、cur_node(selfの場合もあれば他ノードの場合もある)が、生存している状態が通常期待される
                #     # ところで、node_kill_th の処理がノードをダウン状態にしてしまった
                #
                #     ChordUtil.dprint("stabilize_successor_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info)
                #                      + ",TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                #     exception_occured = True
                #     continue
                finally:
                    # 正常に処理が終わる場合、例外処理が発生した場合のいずれも1だけインクリメントが必要なため
                    # ここでそれを行う
                    cur_backup_node_info_idx += 1
                    tried_getting_succ_cnt += 1

            if len(updated_list) == 0:
                # first node の場合の考慮
                # successor_info_listを更新しないで処理を終える
                pass
            else:
                self.existing_node.node_info.successor_info_list = updated_list

            ChordUtil.dprint("stabilize_successor_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + str(self.existing_node.node_info.successor_info_list))

            return PResult.Ok(True)
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()

    # FingerTableに関するstabilize処理を行う
    # 一回の呼び出しで1エントリを更新する
    # FingerTableのエントリはこの呼び出しによって埋まっていく
    # TODO: InternalExp at stabilize_finger_table
    def stabilize_finger_table(self, idx) -> PResult[bool]:
        # if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
        #     ChordUtil.dprint("stabilize_finger_table_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
        #                      + "LOCK_ACQUIRE_TIMEOUT")
        #     #raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
        #     return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
        # if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
        #     self.existing_node.node_info.lock_of_pred_info.release()
        #     ChordUtil.dprint("stabilize_finger_table_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
        #                      + "LOCK_ACQUIRE_TIMEOUT")
        #     #raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")
        #     return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

        if self.existing_node.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            # self.existing_node.node_info.lock_of_succ_infos.release()
            # self.existing_node.node_info.lock_of_pred_info.release()
            if self.existing_node.is_alive == False:
                ChordUtil.dprint(
                    "stabilize_finger_table_0_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
                return PResult.Ok(True)

        # try:
        ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

        ChordUtil.dprint("stabilize_finger_table_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        # FingerTableの各要素はインデックスを idx とすると 2^IDX 先のIDを担当する、もしくは
        # 担当するノードに最も近いノードが格納される
        update_id = ChordUtil.overflow_check_and_conv(self.existing_node.node_info.node_id + 2**idx)
        # try:
            #found_node = self.existing_node.router.find_successor(update_id)
        ret = self.existing_node.router.find_successor(update_id)
        if (ret.is_ok):
            found_node : 'ChordNode' = cast('ChordNode', ret.result)
        else:
            # ret.err_code == ErrorCode.AppropriateNodeNotFoundException_Code || ret.err_code == ErrorCode.InternalControlFlowException_CODE
            #  || ret.err_code == ErrorCode.NodeIsDownedException_CODE

            # 適切な担当ノードを得ることができなかった
            # 今回のエントリの更新はあきらめるが、例外の発生原因はおおむね見つけたノードがダウンしていた
            # ことであるので、更新対象のエントリには None を設定しておく
            self.existing_node.node_info.finger_table[idx] = None
            ChordUtil.dprint("stabilize_finger_table_2_5,NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(
                self.existing_node.node_info))
            return PResult.Ok(True)

        # except (AppropriateNodeNotFoundException, NodeIsDownedExceptiopn, InternalControlFlowException):
        #     # 適切な担当ノードを得ることができなかった
        #     # 今回のエントリの更新はあきらめるが、例外の発生原因はおおむね見つけたノードがダウンしていた
        #     # ことであるので、更新対象のエントリには None を設定しておく
        #     self.existing_node.node_info.finger_table[idx] = None
        #     ChordUtil.dprint("stabilize_finger_table_2_5,NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))
        #     return

        # TODO: x direct access to node_info of found_node at stabilize_finger_table
        self.existing_node.node_info.finger_table[idx] = found_node.node_info.get_partial_deepcopy()

        # TODO: x direct access to node_info of found_node at stabilize_finger_table
        ChordUtil.dprint("stabilize_finger_table_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(found_node.node_info))

        return PResult.Ok(True)
        # finally:
        #     self.existing_node.node_info.lock_of_succ_infos.release()
        #     self.existing_node.node_info.lock_of_pred_info.release()