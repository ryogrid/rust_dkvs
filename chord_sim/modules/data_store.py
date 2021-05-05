# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

from .chord_util import ChordUtil, KeyValue, DataIdAndValue, PResult, ErrorCode
import gval

if TYPE_CHECKING:
    from .chord_node import ChordNode
    from .node_info import NodeInfo

class DataStore:

    DELETED_ENTRY_MARKING_STR = "THIS_KEY_IS_DELETED"
    DATA_STORE_OP_DIRECT_STORE = "DIRECT_STORE"
    DATA_STORE_OP_DIRECT_REMOVE = "DIRECT_REMOVE"

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node : 'ChordNode' = existing_node

        # Keyはハッシュを通されたものなので元データの値とは異なる
        self.stored_data : Dict[str, DataIdAndValue] = {}

    # DataStoreクラスオブジェクトのデータ管理の枠組みに従った、各関連フィールドの一貫性を維持したまま
    # データ追加・更新処理を行うアクセサメソッド
    # master_node引数を指定しなかった場合は、self.existing_node.node_info をデータのマスターの情報として格納する
    def store_new_data(self, data_id : int, value_str : str):
        # ログの量が多くなりすぎるのでコメントアウトしておく
        # ChordUtil.dprint("store_new_data_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
        #                  + ChordUtil.gen_debug_str_of_data(data_id))

        with self.existing_node.node_info.lock_of_datastore:
            di_entry = DataIdAndValue(data_id=data_id, value_data=value_str)

            # デバッグプリント
            ChordUtil.dprint_data_storage_operations(self.existing_node.node_info,
                                                     DataStore.DATA_STORE_OP_DIRECT_STORE,
                                                     data_id
                                                     )

            self.stored_data[str(data_id)] = di_entry
            # デバッグのためにグローバル変数の形で管理されているデータのロケーション情報を更新する
            ChordUtil.add_data_placement_info(data_id, self.existing_node.node_info)

    # # DataStoreクラスオブジェクトのデータ管理の枠組みに従った、各関連フィールドの一貫性を維持したまま
    # # データ削除処理を行うアクセサメソッド
    # def remove_data(self, data_id: int):
    #     with self.existing_node.node_info.lock_of_datastore:
    #         try:
    #             del self.stored_data[str(data_id)]
    #         except KeyError:
    #             # 本来は起きてはならないエラーだが対処のし様もないのでワーニングだけ出力する
    #             ChordUtil.dprint("remove_data_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
    #                              + ChordUtil.gen_debug_str_of_data(data_id)
    #                              + ",WARNING__REMOVE_TARGET_DATA_NOT_EXIST")
    #             return
    #
    #         # デバッグのためにグローバル変数の形で管理されているデータのロケーション情報を更新する
    #         ChordUtil.remove_data_placement_info(data_id, self.existing_node.node_info)
    #         # デバッグプリント
    #         ChordUtil.dprint_data_storage_operations(self.existing_node.node_info,
    #                                                  DataStore.DATA_STORE_OP_DIRECT_REMOVE,
    #                                                  data_id
    #                                                  )

    # 自ノードが担当ノードとなる保持データを全て返す
    def get_all_tantou_data(self, node_id : Optional[int] = None) -> PResult[Optional[List[DataIdAndValue]]]:
        #with self.existing_node.node_info.lock_of_datastore:
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint(
                "get_all_tantou_data_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint(
                "get_all_tantou_data_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")
            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)
        if self.existing_node.node_info.lock_of_datastore.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            self.existing_node.node_info.lock_of_succ_infos.release()
            ChordUtil.dprint(
                "get_all_tantou_data_0_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "LOCK_ACQUIRE_TIMEOUT")
            #raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")
            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

        try:
            ChordUtil.dprint(
                "pass_tantou_data_for_replication_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

            if self.existing_node.node_info.predecessor_info == None and node_id == None:
                ChordUtil.dprint(
                    "pass_tantou_data_for_replication_2," + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info))
                #return []
                return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

            if node_id != None:
                pred_id = cast(int, node_id)
            else:
                pred_id = cast('NodeInfo', self.existing_node.node_info.predecessor_info).node_id

            ret_data_list : List[DataIdAndValue] = []
            for key, value in self.stored_data.items():
                if ChordUtil.exist_between_two_nodes_right_mawari(pred_id, self.existing_node.node_info.node_id, int(key)):
                    ret_data_list.append(DataIdAndValue(data_id=int(key), value_data=value.value_data))

            ChordUtil.dprint("pass_tantou_data_for_replication_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             # + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.predecessor_info) + ","
                             + str(len(ret_data_list)))

            return PResult.Ok(ret_data_list)

        finally:
            self.existing_node.node_info.lock_of_datastore.release()
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()

    # レプリカデータを受け取る
    # 他のノードが、保持しておいて欲しいレプリカを渡す際に呼び出される.
    # なお、master_node 引数と呼び出し元ノードは一致しない場合がある.
    # replace_allオプション引数をTrueとした場合は、指定したノードのデータを丸っと入れ替える
    # 返り値として、処理が完了した時点でmaster_nodeに紐づいているレプリカをいくつ保持して
    # いるかを返す
    def receive_replica(self, pass_datas : List[DataIdAndValue]):
        with self.existing_node.node_info.lock_of_datastore:
            ChordUtil.dprint("receive_replica_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + str(len(pass_datas)))

            for id_value in pass_datas:
                self.store_new_data(id_value.data_id, id_value.value_data)

            ChordUtil.dprint("receive_replica_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + str(len(pass_datas)))

    # 複数マスタのレプリカをまとめて受け取り格納する
    def store_replica_of_multi_masters(self, data_list: List[DataIdAndValue]):
        ChordUtil.dprint(
            "store_replica_of_multi_masters_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + str(len(data_list)))

        self.receive_replica(data_list)

        ChordUtil.dprint(
            "store_replica_of_multi_masters_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + str(len(data_list)))

    # 自身が保持しているデータのうち委譲するものを返す.
    # 対象となるデータは時計周りに辿った際に 引数 node_id と 自身の node_id
    # の間に data_id が位置するデータである.
    # join呼び出し時、新たに参加してきた新規ノードに、successorとなる自身が、担当から外れる
    # 範囲のデータの委譲を行うために、新規ノードから呼び出される形で用いられる.
    # rest_copy引数によってコピーを渡すだけか、完全に委譲してしまい自身のデータストアからは渡したデータを削除
    # するかどうか選択できる
    def delegate_my_tantou_data(self, node_id : int) -> List[KeyValue]:
        with self.existing_node.node_info.lock_of_datastore:
            ChordUtil.dprint("delegate_my_tantou_data_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(node_id))
            ret_datas : List[KeyValue] = []
            #tantou_data: List[DataIdAndValue] = self.get_all_tantou_data(node_id)
            ret = self.get_all_tantou_data(node_id)
            if (ret.is_ok):
                tantou_data: List[DataIdAndValue] = cast(List[DataIdAndValue], ret.result)
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                # TODO: 本来はデータの委譲が失敗することになるので、join処理のリトライを行わせる必要があるが面倒なので、
                #       ひとまず、ここでエラーが返ってくることは想定しないことにする
                tantou_data: List[DataIdAndValue] = []

            for entry in tantou_data:
                # Chordネットワークを右回りにたどった時に、データの id (data_id) が呼び出し元の node_id から
                # 自身の node_id の間に位置する場合は、そのデータの担当は自身から変わらないため、渡すデータから
                # 除外する
                if ChordUtil.exist_between_two_nodes_right_mawari(node_id, self.existing_node.node_info.node_id, entry.data_id):
                    ChordUtil.dprint(
                        "delegate_my_tantou_data_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                        + ChordUtil.gen_debug_str_of_data(node_id) + "," + ChordUtil.gen_debug_str_of_data(entry.data_id))
                    continue

                # 文字列の参照をそのまま用いてしまうが、文字列はイミュータブルであるため
                # 問題ない
                item = KeyValue(None, entry.value_data)
                item.data_id = entry.data_id
                ret_datas.append(item)

        return ret_datas

    # 存在しないKeyが与えられた場合 KeyErrorがraiseされる
    def get(self, data_id : int) -> PResult[Optional[DataIdAndValue]]:
        with self.existing_node.node_info.lock_of_datastore:
            try:
                return PResult.Ok(self.stored_data[str(data_id)])
            except KeyError:
                return PResult.Err(None, ErrorCode.KeyError_CODE)


    # 全ての保持しているデータを返す
    def get_all_data(self) -> List[DataIdAndValue]:
        ChordUtil.dprint("get_all_data_1," + ChordUtil.gen_debug_str_of_node(
            self.existing_node.node_info))

        with self.existing_node.node_info.lock_of_datastore:
            ret_data_list: List[DataIdAndValue] = []
            for key, value in self.stored_data.items():
                ret_data_list.append(DataIdAndValue(data_id=int(key), value_data=value.value_data))

            ChordUtil.dprint("get_all_data_2," + ChordUtil.gen_debug_str_of_node(
                self.existing_node.node_info) + ","
                + str(len(ret_data_list)))

        return ret_data_list

    # 担当データ全てのレプリカを successor_info_list内のノードに配る
    # 必要なロックは呼び出し元でとってある前提
    def distribute_replica(self):
        ChordUtil.dprint("distribute_replica_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        #tantou_data_list: List[DataIdAndValue] = self.get_all_tantou_data()
        ret = self.get_all_tantou_data()
        if (ret.is_ok):
            tantou_data_list: List[DataIdAndValue] = cast(List[DataIdAndValue], ret.result)
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
            # putされた時にいずれ受け取るので良いことにする
            tantou_data_list: List[DataIdAndValue] = []

        # レプリカを successorList内のノードに渡す（手抜きでputされたもの含めた全てを渡してしまう）
        for succ_info in self.existing_node.node_info.successor_info_list:
            # try:
                # succ_node: ChordNode = ChordUtil.get_node_by_address(succ_info.address_str)
            ret2 = ChordUtil.get_node_by_address(succ_info.address_str)
            if (ret2.is_ok):
                succ_node : 'ChordNode' = cast('ChordNode', ret2.result)
            else:  # ret2.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                # stabilize処理 と put処理 を経ていずれ正常な状態に
                # なるため、ここでは何もせずに次のノードに移る
                ChordUtil.dprint(
                    "distribute_replica_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(succ_info))
                continue
            # except (NodeIsDownedExceptiopn, InternalControlFlowException):
            #     # stabilize処理 と put処理 を経ていずれ正常な状態に
            #     # なるため、ここでは何もせずに次のノードに移る
            #     ChordUtil.dprint("distribute_replica_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            #                      + ChordUtil.gen_debug_str_of_node(succ_info))
            #     continue

            # 非効率だが、putやstabilize_successorなどの度に担当データを全て渡してしまう
            # TODO: putやstabilize_successorが呼び出される担当データ全てのレプリカを渡すのはあまりに非効率なので、担当データのIDリストを渡して
            #       持っていないデータのIDのリストを返してもらい、それらのデータのみ渡すようにいずれ修正する

            # TODO: receive_replica call at distribute_replica
            succ_node.endpoints.grpc__receive_replica(tantou_data_list)

            ChordUtil.dprint("distribute_replica_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(succ_info))
