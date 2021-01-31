# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

import modules.gval as gval
from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    TargetNodeDoesNotExistException, StoredValueEntry, NodeInfoPointer, DataIdAndValue

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

    # successor_info_listの長さをチェックし、規定長を越えていた場合余剰なノードにレプリカを
    # 削除させた上で、リストから取り除く
    def check_replication_redunduncy(self):
        ChordUtil.dprint(
            "check_replication_redunduncy_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + str(len(self.existing_node.node_info.successor_info_list)))

        if len(self.existing_node.node_info.successor_info_list) > gval.SUCCESSOR_LIST_NORMAL_LEN:
            for idx in range(gval.SUCCESSOR_LIST_NORMAL_LEN, len(self.existing_node.node_info.successor_info_list)):
                node_info = self.existing_node.node_info.successor_info_list[idx]
                try:
                    successor_node : ChordNode = ChordUtil.get_node_by_address(node_info.address_str)
                    successor_node.data_store.delete_replica(self.existing_node.node_info)
                    ChordUtil.dprint(
                        "check_replication_redunduncy_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                        + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[idx])
                        + str(len(self.existing_node.node_info.successor_info_list)))
                    # 余剰となったノードを successorListから取り除く
                    self.existing_node.node_info.successor_info_list.remove(node_info)
                except NodeIsDownedExceptiopn:
                    # 余剰ノードがダウンしていた場合はここでは何も対処しない
                    ChordUtil.dprint(
                        "check_replication_redunduncy_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                        + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[idx])
                        + str(len(self.existing_node.node_info.successor_info_list)) + ",NODE_IS_DOWNED")
                    # ダウンしているので、レプリカを削除させることはできないが、それが取得されてしまうことも無いため
                    # 特にレプリカに関するケアは行わず、余剰となったノードとして successorListから取り除く
                    self.existing_node.node_info.successor_info_list.remove(node_info)
                    continue

    # node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
    def join(self, node_address : str):
        # 実装上例外は発生しない.
        # また実システムでもダウンしているノードの情報が与えられることは想定しない
        tyukai_node = ChordUtil.get_node_by_address(node_address)
        ChordUtil.dprint("join_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))

        try:
            # 仲介ノードに自身のsuccessorになるべきノードを探してもらう
            successor = tyukai_node.router.find_successor(self.existing_node.node_info.node_id)
            # リトライは不要なので、本メソッドの呼び出し元がリトライ処理を行うかの判断に用いる
            # フィールドをリセットしておく
            Stabilizer.need_join_retry_node = None
        except AppropriateNodeNotFoundException:
            # リトライに必要な情報を記録しておく
            Stabilizer.need_join_retry_node = self.existing_node
            Stabilizer.need_join_retry_tyukai_node = tyukai_node

            # 自ノードの情報、仲介ノードの情報
            ChordUtil.dprint("join_2,RETRY_IS_NEEDED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))
            return

        self.existing_node.node_info.successor_info_list.append(successor.node_info.get_partial_deepcopy())

        # successorから自身が担当することになるID範囲のデータの委譲を受け、格納する
        tantou_data_list : List[KeyValue] = successor.data_store.delegate_my_tantou_data(self.existing_node.node_info.node_id, False)
        for key_value in tantou_data_list:
            self.existing_node.data_store.store_new_data(cast(int, key_value.data_id), key_value.value_data)

        # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
        self.existing_node.node_info.finger_table[0] = self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()

        if tyukai_node.node_info.node_id == tyukai_node.node_info.successor_info_list[0].node_id:
            # secondノードの場合の考慮 (仲介ノードは必ずfirst node)

            predecessor = tyukai_node

            # 2ノードでsuccessorでもpredecessorでも、チェーン構造で正しい環が構成されるよう強制的に全て設定してしまう
            self.existing_node.node_info.predecessor_info = predecessor.node_info.get_partial_deepcopy()
            tyukai_node.node_info.predecessor_info = self.existing_node.node_info.get_partial_deepcopy()
            tyukai_node.node_info.successor_info_list[0] = self.existing_node.node_info.get_partial_deepcopy()
            # fingerテーブルの0番エントリも強制的に設定する
            tyukai_node.node_info.finger_table[0] = self.existing_node.node_info.get_partial_deepcopy()


            ChordUtil.dprint("join_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))
        else:
            # 強制的に自身を既存のチェーンに挿入する
            # successorは predecessorの 情報を必ず持っていることを前提とする
            self.existing_node.node_info.predecessor_info = cast('NodeInfo', successor.node_info.predecessor_info).get_partial_deepcopy()
            successor.node_info.predecessor_info = self.existing_node.node_info.get_partial_deepcopy()

            # 例外発生時は取得を試みたノードはダウンしているが、無視してpredecessorに設定したままにしておく.
            # 不正な状態に一時的になるが、predecessorをsuccessor_info_listに持つノードが
            # stabilize_successorを実行した時点で解消されるはず
            try:
                predecessor = ChordUtil.get_node_by_address(cast('NodeInfo', self.existing_node.node_info.predecessor_info).address_str)
                predecessor.node_info.successor_info_list.insert(0, self.existing_node.node_info.get_partial_deepcopy())

                # successorListを埋めておく
                self.existing_node.stabilizer.stabilize_successor()

                ChordUtil.dprint("join_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                                 + ChordUtil.gen_debug_str_of_node(predecessor.node_info))
            except NodeIsDownedExceptiopn:
                # ここでは特に何も対処しない
                ChordUtil.dprint("join_5,NODE_IS_DOWNED" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))
                pass

        # successor[0] から委譲を受けたデータを successorList 内の全ノードにレプリカとして配る
        for node_info in self.existing_node.node_info.successor_info_list:
            try:
                node = ChordUtil.get_node_by_address(node_info.address_str)
                ChordUtil.dprint("join_6," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(node_info) + "," + str(len(self.existing_node.node_info.successor_info_list)))
                node.data_store.receive_replica(
                    self.existing_node.node_info,
                    [DataIdAndValue(data_id = cast('int', data.data_id), value_data=data.value_data) for data in tantou_data_list]
                )
            except NodeIsDownedExceptiopn:
                # ノードがダウンしていた場合は無視して次のノードに進む.
                # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                # put処理 の中で後ほど行われるためここでは対処しない
                ChordUtil.dprint("join_7,NODE_IS_DOWNED" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(node_info))
                continue

        if self.existing_node.node_info.predecessor_info != None:
            self_predecessor_info : NodeInfo = cast('NodeInfo', self.existing_node.node_info.predecessor_info)
            try:
                # predecessorが非Noneであれば当該ノードの担当データをレプリカとして保持しておかなければならないため
                # データを渡してもらい、格納する
                self_predeessor_node = ChordUtil.get_node_by_address(self_predecessor_info.address_str)
                pred_tantou_datas : List[DataIdAndValue] = self_predeessor_node.data_store.pass_tantou_data_for_replication()
                for iv_entry in pred_tantou_datas:
                    self.existing_node.data_store.store_new_data(iv_entry.data_id,
                                                                 iv_entry.value_data,
                                                                 master_info=self_predecessor_info.get_partial_deepcopy()
                                                                 )
                ChordUtil.dprint("join_8," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self_predeessor_node.node_info) + "," + str(len(pred_tantou_datas)))

                # predecessor が非Noneであれば、当該predecessorのsuccessor_info_listの長さが標準を越えてしまって
                # いる場合があるため、そのチェックと、越えていた場合の余剰のノードからレプリカを全て削除させる処理を呼び出す
                # (この呼び出しの中で successorListからの余剰ノード情報削除も行われる）
                self_predeessor_node.stabilizer.check_replication_redunduncy()
            except NodeIsDownedExceptiopn:
                ChordUtil.dprint("join_9,NODE_IS_DOWNED" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(node_info))
                # ノードがダウンしていた場合は無視して次のノードに進む.
                # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                # put処理 の中で後ほど行われるためここでは対処しない
                pass

        # successorから保持している全てのレプリカを受け取り格納する（successorよりは前に位置することになるため、
        # 基本的にsuccessorが保持しているレプリカは自身も全て保持している状態とならなければならない）
        passed_all_replica: Dict[NodeInfo, List[DataIdAndValue]] = successor.data_store.pass_all_replica()
        self.existing_node.data_store.store_replica_of_several_masters(passed_all_replica)

        # 自ノードの情報、仲介ノードの情報、successorとして設定したノードの情報
        ChordUtil.dprint("join_10," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
    # Attention: TargetNodeDoesNotExistException を raiseする場合がある
    def check_predecessor(self, id : int, node_info : 'NodeInfo'):
        ChordUtil.dprint("check_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

        # この時点で認識している predecessor がノードダウンしていないかチェックする
        is_pred_alived = ChordUtil.is_node_alive(cast('NodeInfo', self.existing_node.node_info.predecessor_info).address_str)

        if is_pred_alived:
            distance_check = ChordUtil.calc_distance_between_nodes_left_mawari(self.existing_node.node_info.node_id, id)
            distance_cur = ChordUtil.calc_distance_between_nodes_left_mawari(self.existing_node.node_info.node_id, cast('NodeInfo',self.existing_node.node_info.predecessor_info).node_id)

            # 確認を求められたノードの方が現在の predecessor より predecessorらしければ
            # 経路表の情報を更新する
            if distance_check < distance_cur:
                self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()

                ChordUtil.dprint("check_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                      + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                      + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.predecessor_info))
        else: # predecessorがダウンしていた場合は無条件でチェックを求められたノードをpredecessorに設定する
            self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()

    #  ノードダウンしておらず、チェーンの接続関係が正常 (predecessorの情報が適切でそのノードが生きている)
    #  なノードで、諸々の処理の結果、self の successor[0] となるべきノードであると確認されたノードを返す.
    #　注: この呼び出しにより、self.existing_node.node_info.successor_info_list[0] は更新される
    #  規約: 呼び出し元は、selfが生きていることを確認した上で本メソッドを呼び出さなければならない
    def stabilize_successor_inner(self) -> 'NodeInfo':
        # 本メソッド呼び出しでsuccessorとして扱うノードはsuccessorListの先頭から生きているもの
        # をサーチし、発見したノードとする.
        ChordUtil.dprint("stabilize_successor_inner_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        successor : 'ChordNode'
        successor_tmp : Optional['ChordNode'] = None
        for idx in range(len(self.existing_node.node_info.successor_info_list)):
            try:
                if ChordUtil.is_node_alive(self.existing_node.node_info.successor_info_list[idx].address_str):
                    successor_tmp = ChordUtil.get_node_by_address(self.existing_node.node_info.successor_info_list[idx].address_str)
                    break
                else:
                    ChordUtil.dprint("stabilize_successor_inner_1,SUCCESSOR_IS_DOWNED,"
                                     + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[idx]))
            except TargetNodeDoesNotExistException:
                # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                # した際に発生してしまうので、ここで対処する
                # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなので、後続の処理を行わず successor[0]を返す
                return self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()

        if successor_tmp != None:
            successor = cast('ChordNode', successor_tmp)
        else:
            # successorListの全てのノードを当たっても、生きているノードが存在しなかった場合
            # 起きてはいけない状況なので例外を投げてプログラムを終了させる
            raise Exception("Maybe some parameters related to fault-tolerance of Chord network are not appropriate")

        # 生存が確認されたノードを successor[0] として設定する
        self.existing_node.node_info.successor_info_list[0] = successor.node_info.get_partial_deepcopy()

        ChordUtil.dprint("stabilize_successor_inner_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

        pred_id_of_successor = cast('NodeInfo', successor.node_info.predecessor_info).node_id

        ChordUtil.dprint("stabilize_successor_inner_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                         + str(pred_id_of_successor))

        # successorが認識している predecessor の情報をチェックさせて、適切なものに変更させたり、把握していない
        # 自身のsuccessor[0]になるべきノードの存在が判明した場合は 自身の successor[0] をそちらに張り替える.
        # なお、下のパターン1から3という記述は以下の資料による説明に基づく
        # https://www.slideshare.net/did2/chorddht
        if(pred_id_of_successor == self.existing_node.node_info.node_id):
            # パターン1
            # 特に訂正は不要
            ChordUtil.dprint("stabilize_successor_inner_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                             + str(pred_id_of_successor))
        else:
            # 以下、パターン2およびパターン3に対応する処理

            try:
                # 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
                # 情報を更新してもらう
                # 注: successorが認識していた predecessorがダウンしていた場合、下の呼び出しにより後続でcheck_predecessorを
                #     を呼び出すまでもなく、successorのpredecessorは自身になっている. 従って後続でノードダウン検出した場合の
                #     check_predecessorの呼び出しは不要であるが呼び出しは行うようにしておく
                successor.stabilizer.check_predecessor(self.existing_node.node_info.node_id, self.existing_node.node_info)

                distance_unknown = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id, pred_id_of_successor)
                distance_me = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id, self.existing_node.node_info.node_id)
                if distance_unknown < distance_me:
                    # successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
                    # successorから自身に対して前方向にたどった場合の経路中に存在する場合
                    # 自身の認識するsuccessorの情報を更新する

                    try:
                        new_successor = ChordUtil.get_node_by_address(cast('NodeInfo', successor.node_info.predecessor_info).address_str)
                        self.existing_node.node_info.successor_info_list.insert(0, new_successor.node_info.get_partial_deepcopy())

                        # 新たなsuccesorに対して担当データのレプリカを渡す
                        tantou_data_list : List[DataIdAndValue] = \
                            self.existing_node.data_store.get_all_replica_by_master_node(self.existing_node.node_info.node_id)
                        new_successor.data_store.receive_replica(self.existing_node.node_info.get_partial_deepcopy(),
                                                                 tantou_data_list, replace_all=True)

                        # successorListから溢れたノードがいた場合、自ノードの担当データのレプリカを削除させ、successorListから取り除く
                        # (この呼び出しの中でsuccessorListからのノード情報の削除も行われる)
                        self.check_replication_redunduncy()

                        # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
                        # ば情報を更新してもらう
                        new_successor.stabilizer.check_predecessor(self.existing_node.node_info.node_id, self.existing_node.node_info)

                        ChordUtil.dprint("stabilize_successor_inner_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                                         + ChordUtil.gen_debug_str_of_node(new_successor.node_info))
                    except NodeIsDownedExceptiopn:
                        # 例外発生時は張り替えを中止する
                        #   - successorは変更しない
                        #   - この時点でのsuccessor[0]が認識するpredecessorを自身とする(successr[0]のcheck_predecessorを呼び出す)

                        # successor[0]の変更は行わず、ダウンしていたノードではなく自身をpredecessorとするよう(間接的に)要請する
                        successor.stabilizer.check_predecessor(self.existing_node.node_info.node_id, self.existing_node.node_info)
                        ChordUtil.dprint("stabilize_successor_inner_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            except TargetNodeDoesNotExistException:
                # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                # した際にcheck_predecessorで発生する場合があるので、ここで対処する
                # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなの特に処理は不要であり
                # 本メソッドは元々の successor[0] を返せばよい
                ChordUtil.dprint("stabilize_successor_inner_6," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

        return self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()


    # successorListに関するstabilize処理を行う
    # コメントにおいては、successorListの構造を意識した記述の場合、一番近いsuccessorを successor[0] と
    # 記述し、以降に位置するノードは近い順に successor[idx] と記述する
    def stabilize_successor(self):
        # TODO: put時にレプリカを全て、もしくは一部持っていないノードについてはケアされる
        #       ため、大局的には問題ないと思われるが、ノードダウンを検出した場合や、未認識
        #       であったノードを発見した場合に、レプリカの配置状態が前述のケアでカバーできない
        #       ような状態とならないか確認する
        #       on stabilize_successor
        #       for 契機4

        ChordUtil.dprint("stabilize_successor_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
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

        while len(updated_list) < gval.SUCCESSOR_LIST_NORMAL_LEN:
            cur_node_info : 'NodeInfo' = cur_node.stabilizer.stabilize_successor_inner()
            ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(cur_node_info))
            if cur_node_info.node_id == self.existing_node.node_info.node_id:
                # Chordネットワークに (downしていない状態で) 存在するノード数が gval.SUCCESSOR_LIST_NORMAL_LEN
                # より少ない場合 successorをたどっていった結果、自ノードにたどり着いてしまうため、その場合は規定の
                # ノード数を満たしていないが、successor_info_list の更新処理は終了する
                ChordUtil.dprint("stabilize_successor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(cur_node_info))
                if len(updated_list) == 0:
                    # first node の場合の考慮
                    # second node が 未joinの場合、successsor[0] がリストに存在しない状態となってしまうため
                    # その場合のみ、updated_list で self.existing_node.node_info.successor_info_listを上書きせずにreturnする
                    ChordUtil.dprint("stabilize_successor_2_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(cur_node_info))
                    return

                break

            updated_list.append(cur_node_info)
            # この呼び出しで例外は発生しない
            cur_node = ChordUtil.get_node_by_address(cur_node_info.address_str)

        self.existing_node.node_info.successor_info_list = updated_list
        ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                     + str(self.existing_node.node_info.successor_info_list))

    # FingerTableに関するstabilize処理を行う
    # 一回の呼び出しで1エントリを更新する
    # FingerTableのエントリはこの呼び出しによって埋まっていく
    def stabilize_finger_table(self, idx):
        ChordUtil.dprint("stabilize_finger_table_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        # FingerTableの各要素はインデックスを idx とすると 2^IDX 先のIDを担当する、もしくは
        # 担当するノードに最も近いノードが格納される
        update_id = ChordUtil.overflow_check_and_conv(self.existing_node.node_info.node_id + 2**idx)
        try:
            found_node = self.existing_node.router.find_successor(update_id)
        except AppropriateNodeNotFoundException:
            # 適切な担当ノードを得ることができなかった
            # 今回のエントリの更新はあきらめるが、例外の発生原因はおおむね見つけたノードがダウンしていた
            # ことであるので、更新対象のエントリには None を設定しておく
            self.existing_node.node_info.finger_table[idx] = None
            ChordUtil.dprint("stabilize_finger_table_2_5,NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))
            return

        self.existing_node.node_info.finger_table[idx] = found_node.node_info.get_partial_deepcopy()

        ChordUtil.dprint("stabilize_finger_table_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(found_node.node_info))