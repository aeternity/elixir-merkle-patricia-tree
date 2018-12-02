defmodule MerklePatriciaTree.BuilderTest do
  use ExUnit.Case, async: true
  doctest MerklePatriciaTree.Trie.Builder

  alias MerklePatriciaTree.Trie
  alias MerklePatriciaTree.Trie.Builder
  alias MerklePatriciaTree.Trie.Node
  alias MerklePatriciaTree.Trie.Helper
  alias MerklePatriciaTree.ListHelper
  alias MerklePatriciaTree.Trie.Storage
  alias MerklePatriciaTree.DB.ETS

  setup do
    db = ETS.random_ets_db()
    trie = Trie.new(db)

    {:ok, %{trie: trie}}
  end

  defp store_node(node, trie) do
    Node.encode_node(node, trie)
      |> Trie.into(trie)
      |> Trie.store
  end

  test "Put key into empty trie", %{trie: trie} do
    key = [1, 2, 3]
    value = "value"

    node = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)

    assert node == {:leaf, key, value}
  end

  test "Put key into trie with only leaf node with identical key", %{trie: trie} do
    key = Helper.get_nibbles("key")
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    value2 = "value2"
    node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key, value2, one_leaf_trie)

    assert node == {:leaf, key, value2}
  end
  
  test "Put key into trie with a leaf node sharing nothing", %{trie: trie} do
    key = [1, 2, 3]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [2, 3, 4]
    value2 = "value2"
    branch_node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:branch, branch_options} = branch_node
    assert length(branch_options) == 17

    value_hash = Enum.at(branch_options, Enum.at(key, 0))
    value2_hash = Enum.at(branch_options, Enum.at(key2, 0))

    two_leaf_trie = branch_node
      |> store_node(one_leaf_trie)

    [node_prefix_tl, node_value] = Trie.into(value_hash, two_leaf_trie)
      |> Storage.get_node()
      |> ExRLP.decode()

    [node2_prefix_tl, node2_value] = Trie.into(value2_hash, two_leaf_trie)
      |> Storage.get_node()
      |> ExRLP.decode()

    { hexprefix_tl1, true } = HexPrefix.decode(node_prefix_tl)
    { hexprefix_tl2, true } = HexPrefix.decode(node2_prefix_tl)

    assert hexprefix_tl1 == [2, 3]
    assert node_value == "value"

    assert hexprefix_tl2 == [3, 4]
    assert node2_value == "value2"
  end

  test "Put key into trie containing a leaf node with longer key", %{trie: trie} do
    key = Helper.get_nibbles("keys")
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = Helper.get_nibbles("ke")
    value2 = "value2"

    node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, matching_prefix, branch_hash} = node
    assert matching_prefix == key2

    two_leaf_trie = node
      |> store_node(one_leaf_trie)
    branch_node = Trie.into(branch_hash, two_leaf_trie)
      |> Storage.get_node()

    assert length(branch_node) == 17
    assert Enum.at(branch_node, 16) == value2

    [hd | tl] = ListHelper.get_postfix(key, key2)
    leaf_hash = Enum.at(branch_node, hd)
    assert byte_size(leaf_hash) < 32

    leaf_node = ExRLP.decode(leaf_hash)

    {:leaf, leaf_prefix, leaf_value} = Trie.into(leaf_node, two_leaf_trie)
      |> Node.decode_trie()

    assert leaf_prefix == tl
    assert leaf_value == value
  end

  test "Put key into trie containing a leaf node with shorter key", %{trie: trie} do
    key = Helper.get_nibbles("ke")
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = Helper.get_nibbles("keys")
    value2 = "value2"

    node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, matching_prefix, branch_hash} = node
    assert matching_prefix == key

    two_leaf_trie = node
      |> store_node(one_leaf_trie)
    branch_node = Trie.into(branch_hash, two_leaf_trie)
      |> Storage.get_node()

    assert length(branch_node) == 17
    assert Enum.at(branch_node, 16) == value

    [hd | tl] = ListHelper.get_postfix(key2, key)
    leaf_hash = Enum.at(branch_node, hd)
    assert byte_size(leaf_hash) < 32

    leaf_node = ExRLP.decode(leaf_hash)

    {:leaf, leaf_prefix, leaf_value} = Trie.into(leaf_node, two_leaf_trie)
      |> Node.decode_trie()

    assert leaf_prefix == tl
    assert leaf_value == value2
  end

  test "Put key into trie with a leaf node sharing common prefix", %{trie: trie} do
    key = [1, 2, 3, 4]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2, 5, 6]
    value2 = "value2"

    node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, matching_prefix, branch_hash} = node
    assert matching_prefix == [1, 2]

    two_leaf_trie = node
      |> store_node(one_leaf_trie)
    branch_node = Trie.into(branch_hash, two_leaf_trie)
      |> Storage.get_node()

    assert length(branch_node) == 17

    # First leaf
    leaf_hash = Enum.at(branch_node, 3)
    assert byte_size(leaf_hash) < 32

    leaf_node = ExRLP.decode(leaf_hash)

    {:leaf, leaf_prefix, leaf_value} = Trie.into(leaf_node, two_leaf_trie)
      |> Node.decode_trie()

    assert leaf_prefix == [4]
    assert leaf_value == value

    # Second leaf
    leaf2_hash = Enum.at(branch_node, 5)
    assert byte_size(leaf2_hash) < 32

    leaf2_node = ExRLP.decode(leaf2_hash)

    {:leaf, leaf2_prefix, leaf2_value} = Trie.into(leaf2_node, two_leaf_trie)
      |> Node.decode_trie()

    assert leaf2_prefix == [6]
    assert leaf2_value == value2
  end

  test "Put key into trie containing a ext node with exact prefix", %{trie: trie} do
    # Prepare trie
    key = Helper.get_nibbles("ke")
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = Helper.get_nibbles("keys")
    value2 = "value2"

    node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, matching_prefix, branch_hash} = node

    two_leaf_trie = node
      |> store_node(one_leaf_trie)

    # Update prepared ext node with exact prefix
    updated_value = "updated_value"
    updated_ext_node = Node.decode_trie(two_leaf_trie)
      |> Builder.put_key(key, updated_value, two_leaf_trie)

    {:ext, updated_matching_prefix, updated_branch_hash} = updated_ext_node
    assert updated_matching_prefix == matching_prefix
    assert updated_branch_hash != branch_hash

    # Check updated branch node
    updated_two_leaf_trie = updated_ext_node
      |> store_node(two_leaf_trie)
    updated_branch_node = Trie.into(updated_branch_hash, updated_two_leaf_trie)
      |> Storage.get_node()

    assert length(updated_branch_node) == 17
    assert Enum.at(updated_branch_node, 16) == updated_value
  end

  test "Put key into trie containing a ext node with 1-nibble shorter prefix", %{trie: trie} do
    # Prepare trie
    key = [1, 2]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2, 3, 4]
    value2 = "value2"

    node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, matching_prefix, branch_hash} = node

    two_leaf_trie = node
      |> store_node(one_leaf_trie)

    # Update prepared ext node with 1-nibble longer key
    longer_key = [1, 2, 5]
    value3 = "value3"
    updated_ext_node = Node.decode_trie(two_leaf_trie)
      |> Builder.put_key(longer_key, value3, two_leaf_trie)

    {:ext, updated_matching_prefix, updated_branch_hash} = updated_ext_node
    assert updated_matching_prefix == matching_prefix
    assert updated_branch_hash != branch_hash

    # Check updated branch node
    three_leaf_trie = updated_ext_node
      |> store_node(two_leaf_trie)
    updated_branch_node = Trie.into(updated_branch_hash, three_leaf_trie)
      |> Storage.get_node()

    assert length(updated_branch_node) == 17

    # New leaf
    leaf3_hash = Enum.at(updated_branch_node, 5)
    assert byte_size(leaf3_hash) < 32

    leaf3_node = ExRLP.decode(leaf3_hash)

    {:leaf, leaf3_prefix, leaf3_value} = Trie.into(leaf3_node, three_leaf_trie)
      |> Node.decode_trie()

    assert leaf3_prefix == []
    assert leaf3_value == value3
  end

  test "Put key into trie containing a ext node with 3-nibble shorter prefix", %{trie: trie} do
    # Prepare trie
    key = [1, 2]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2, 3, 4]
    value2 = "value2"

    node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, matching_prefix, branch_hash} = node

    two_leaf_trie = node
      |> store_node(one_leaf_trie)

    # Update prepared ext node with 1-nibble longer key
    longer_key = [1, 2, 5, 6, 7]
    value3 = "value3"
    updated_ext_node = Node.decode_trie(two_leaf_trie)
      |> Builder.put_key(longer_key, value3, two_leaf_trie)

    {:ext, updated_matching_prefix, updated_branch_hash} = updated_ext_node
    assert updated_matching_prefix == matching_prefix
    assert updated_branch_hash != branch_hash

    # Check updated branch node
    three_leaf_trie = updated_ext_node
      |> store_node(two_leaf_trie)
    updated_branch_node = Trie.into(updated_branch_hash, three_leaf_trie)
      |> Storage.get_node()

    assert length(updated_branch_node) == 17

    # New leaf
    leaf3_hash = Enum.at(updated_branch_node, 5)
    assert byte_size(leaf3_hash) < 32

    leaf3_node = ExRLP.decode(leaf3_hash)

    {:leaf, leaf3_prefix, leaf3_value} = Trie.into(leaf3_node, three_leaf_trie)
      |> Node.decode_trie()

    assert leaf3_prefix == [6, 7]
    assert leaf3_value == value3
  end

  test "Put no match key into trie containing an ext node with 1-nibble prefix", %{trie: trie} do
    # Prepare trie
    key = [1]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2]
    value2 = "value2"

    ext_node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, matching_prefix, _branch_hash} = ext_node
    assert matching_prefix == key

    two_leaf_trie = store_node(ext_node, one_leaf_trie)

    # Update prepared ext node with key sharing nothing
    # Produce a new branch node with a branch node directly under it
    key3 = [5]
    value3 = "value3"
    old_ext_node = {:ext, _prefix, decoded_old_branch } = Node.decode_trie(two_leaf_trie)

    new_branch_node = {:branch, branch_options} = Builder.put_key(old_ext_node, key3, value3, two_leaf_trie)

    assert Enum.at(branch_options, 1) == decoded_old_branch

    # New leaf
    leaf3_hash = Enum.at(branch_options, 5)
    assert byte_size(leaf3_hash) < 32

    three_leaf_trie = store_node(new_branch_node, two_leaf_trie)
    leaf3_node = ExRLP.decode(leaf3_hash)

    {:leaf, leaf3_prefix, leaf3_value} = Trie.into(leaf3_node, three_leaf_trie)
      |> Node.decode_trie()

    assert leaf3_prefix == []
    assert leaf3_value == value3
  end

  test "Put no match key into trie containing an ext node with 2-nibble prefix", %{trie: trie} do
    # Prepare trie
    key = [1, 2]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2, 3]
    value2 = "value2"

    ext_node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, matching_prefix, _branch_hash} = ext_node
    assert matching_prefix == key

    two_leaf_trie = store_node(ext_node, one_leaf_trie)

    # Update prepared ext node with key sharing nothing
    # Produce a new branch node with an ext node under it
    key3 = [5, 6]
    value3 = "value3"
    old_ext_node = {:ext, _prefix, decoded_old_branch } = Node.decode_trie(two_leaf_trie)

    new_branch_node = {:branch, branch_options} = Builder.put_key(old_ext_node, key3, value3, two_leaf_trie)
    three_leaf_trie = store_node(new_branch_node, two_leaf_trie)

    # New Ext
    new_ext_hash = Enum.at(branch_options, 1)

    {:ext, old_branch_prefix, old_branch_value} = Trie.into(new_ext_hash, three_leaf_trie)
      |> Node.decode_trie()

    assert old_branch_prefix == [2]
    assert decoded_old_branch == old_branch_value

    # New leaf
    new_leaf_hash = Enum.at(branch_options, 5)
    new_leaf_node = ExRLP.decode(new_leaf_hash)

    assert byte_size(new_leaf_hash) < 32
    assert new_leaf_node == ["6", value3]
  end
end
