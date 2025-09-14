```
SSTable文件格式:
 [Data Block 1]
 [Data Block 2]
 ...
 [Index Block]
 [Bloom Filter]
 [Footer] (8B index_offset, 8B bloom_offset, 8B magic_number)
具体展开：
 [Data Block](有多个Data Block，但其实连续的Blocks之间都是一个个的Entry)
 ├── [Entry 1]
 │   ├── key_length (4B int)
 │   ├── key_data (variable)
 │   ├── value_length (4B int)
 │   └── value_data (variable)
 ├── [Entry 2]
 │   ├── key_length (4B int)
 │   ├── key_data (variable)
 │   ├── value_length (4B int)
 │   └── value_data (variable)
 └── ...
 [Index Block](为每个Data Block生成一条索引记录)
 ├── index_count (4B int)
 ├── [Index Entry 1]
 │   ├── last_key_length (4B int)
 │   ├── last_key_data (variable)
 │   ├── block_offset (8B long)
 │   └── block_size (4B int)
 ├── [Index Entry 2]
 │   ├── last_key_length (4B int)
 │   ├── last_key_data (variable)
 │   ├── block_offset (8B long)
 │   └── block_size (4B int)
 └── ...
 [Bloom Filter] 具体格式由Guava库定义，包含位数组和哈希函数信息
 [Footer]
 ├── index_offset (8B long)
 ├── bloom_offset (8B long)
 └── magic_number (8B long = 0x123456789ABCDEF0L)
```





### 场景设定

假设我们的数据库当前处于以下状态，并且L0的合并阈值 `L0_COMPACTION_THRESHOLD` 为 `5`。

**磁盘上的SSTable文件:**

- **Level 0 (L0层)**: 已经有5个文件，达到了合并阈值。ID越大表示文件越新。
  - `0-10.sst`: `{ "apple": "v1", "banana": "v1" }`
  - `0-11.sst`: `{ "cherry": "v1", "date": "v1" }`
  - `0-12.sst`: `{ "apple": "v2" }`  *(更新了 apple)*
  - `0-13.sst`: `{ "banana": "TOMBSTONE", "fig": "v1" }` *(删除了 banana, 新增了 fig)*
  - `0-14.sst`: `{ "grape": "v1" }`
- **Level 1 (L1层)**: L1层的文件之间key不重叠。
  - `1-5.sst`: `{ "elderberry": "v1", "fig": "v0" }` *(注意：它的key范围与L0有重叠)*
  - `1-8.sst`: `{ "kiwi": "v1", "lemon": "v1" }` *(它的key范围与L0无重叠)*

`TOMBSTONE` 是一个特殊的标记，代表该键已被删除。

------



### 合并过程详解

#### 第1步：触发合并

- **条件检查**: `SSTableManager` 的 `compact()` 方法被调用。它检查L0层的文件数量。
- **当前状态**: `levels.get(0).size()` 的值为 `5`。
- **判断**: `5 >= L0_COMPACTION_THRESHOLD` (5)，条件成立。
- **动作**:
  1. **获取写锁**: `metadataLock.writeLock().lock()`。整个系统进入“只读”或“暂停服务”状态，确保合并期间元数据和文件不会被修改。
  2. 开始合并流程。

#### 第2步：选择参与合并的文件

- **目标**: 确定哪些文件需要被合并。
- **数据结构**: `List<SSTableReader> filesToCompact`
- **过程**:
  1. 将 **L0层的所有文件** 加入 `filesToCompact` 列表。
     - `filesToCompact` = `[ reader(0-10.sst), reader(0-11.sst), reader(0-12.sst), reader(0-13.sst), reader(0-14.sst) ]`
  2. 计算L0层文件总的键范围 (minKey, maxKey)。
     - 遍历L0文件中的所有键：`apple`, `banana`, `cherry`, `date`, `fig`, `grape`。
     - `minKey` = `"apple"`
     - `maxKey` = `"grape"`
  3. 遍历 **L1层的文件**，检查它们的键范围是否与 `[minKey, maxKey]` 有交集。
     - 检查 `1-5.sst` (范围: `["elderberry", "fig"]`)：`"fig"` 在 `["apple", "grape"]` 范围内，存在交集。因此，将 `1-5.sst` 也加入 `filesToCompact`。
     - 检查 `1-8.sst` (范围: `["kiwi", "lemon"]`)：`"kiwi"` > `"grape"`，无交集。因此，`1-8.sst` 不参与本次合并。
- **最终结果**:
  - `filesToCompact` 列表包含6个文件的Reader：`[ reader(0-10.sst), reader(0-11.sst), reader(0-12.sst), reader(0-13.sst), reader(0-14.sst), reader(1-5.sst) ]`



#### 第3步：多路归并排序 (`mergeAndWriteStream`)

这是最核心的步骤。它会读取所有待合并文件的数据，并写入一个全新的SSTable文件。

- **目标**: 生成一个有序、无重复、无已删除数据的流，并写入新文件。

- **关键数据结构**: `PriorityQueue<IteratorEntry> pq` (优先队列/最小堆)。

  - `IteratorEntry` 对象包含：`{ Node<String, String> node, SSTableIterator iterator, long fileId }`
  - `PriorityQueue` 的排序规则:
    1. 首先按 `node.getKey()` 字典序升序排列。
    2. 如果 `key` 相同，则按 `fileId` **降序**排列 (ID大的文件是新文件，优先级更高)。

- **过程模拟**:

  1. **初始化**:
     - 为 `filesToCompact` 中的6个文件分别创建一个迭代器。
     - 从每个迭代器读取第一个条目，封装成 `IteratorEntry` 放入 `pq`。
     - `pq` 的初始内容 (已按优先级排序):
       1. `{ key: "apple", val: "v2", fileId: 12 }`  *(因为 fileId 12 > 10)*
       2. `{ key: "apple", val: "v1", fileId: 10 }`
       3. `{ key: "banana", val: "TOMBSTONE", fileId: 13 }` *(因为 fileId 13 > 10)*
       4. `{ key: "banana", val: "v1", fileId: 10 }`
       5. `{ key: "cherry", val: "v1", fileId: 11 }`
       6. `{ key: "date", val: "v1", fileId: 11 }`
       7. `{ key: "elderberry", val: "v1", fileId: 5 }`
       8. `{ key: "fig", val: "v1", fileId: 13 }` *(因为 fileId 13 > 5)*
       9. `{ key: "fig", val: "v0", fileId: 5 }`
       10. `{ key: "grape", val: "v1", fileId: 14 }`
  2. **循环处理 (从 `pq` 中取出最小元素)**:
     - **循环1**:
       - `pq.poll()`: 取出队首 `{ key: "apple", val: "v2", fileId: 12 }`。
       - `lastKey` 此时为 `null`，不相等。
       - 值不是 `TOMBSTONE`。
       - **动作**: 将 `{"apple": "v2"}` 写入新的SSTable文件。`lastKey` 更新为 `"apple"`。
       - `0-12.sst` 的迭代器已无更多元素。
     - **循环2**:
       - `pq.poll()`: 取出队首 `{ key: "apple", val: "v1", fileId: 10 }`。
       - `lastKey` 为 `"apple"`，与当前key相等。
       - **动作**: **跳过**。这是去重逻辑，因为我们已经写入了更新的版本。
       - `0-10.sst` 的迭代器前进，将其下一个元素 `{"banana": "v1"}` 放入 `pq`。
     - **循环3**:
       - `pq.poll()`: 取出队首 `{ key: "banana", val: "TOMBSTONE", fileId: 13 }`。
       - `lastKey` 为 `"apple"`，不相等。
       - 值是 `TOMBSTONE`。
       - **动作**: **跳过**。这是清理已删除数据的逻辑。`lastKey` 更新为 `"banana"`。
       - `0-13.sst` 的迭代器前进，将其下一个元素 `{"fig": "v1"}` 放入 `pq`。
     - **循环4**:
       - `pq.poll()`: 取出队首 `{ key: "banana", val: "v1", fileId: 10 }`。
       - `lastKey` 为 `"banana"`，相等。
       - **动作**: **跳过**。
       - `0-10.sst` 的迭代器已无更多元素。
     - **...后续循环**:
       - 处理 `cherry` -> 写入 `{"cherry": "v1"}`
       - 处理 `date` -> 写入 `{"date": "v1"}`
       - 处理 `elderberry` -> 写入 `{"elderberry": "v1"}`
       - 处理 `fig` (fileId: 13) -> 写入 `{"fig": "v1"}`
       - 处理 `fig` (fileId: 5) -> 跳过
       - 处理 `grape` -> 写入 `{"grape": "v1"}`

- 最终写入新文件的数据:

  { "apple": "v2", "cherry": "v1", "date": "v1", "elderberry": "v1", "fig": "v1", "grape": "v1" }



#### 第4步：构建新的SSTable文件

当上述循环结束后，一个包含合并后数据的临时文件已经生成。现在需要为其添加元数据，完成最终的SSTable文件构建。假设新文件名为 `1-15.sst`。

- **Data Blocks**: 上一步写入的数据会按约 `DATA_BLOCK_SIZE_TARGET` 的大小被切分成多个数据块。
  - `[Data Block 1]`: `{ "apple": "v2", "cherry": "v1", ... }`
  - `[Data Block 2]`: `{ ..., "fig": "v1", "grape": "v1" }`
- **Index Block**: 为每个Data Block生成一条索引记录。
  - `[Index Entry 1]`: `{ last_key: "date", block_offset: 0, block_size: ... }`
  - `[Index Entry 2]`: `{ last_key: "grape", block_offset: ..., block_size: ... }`
- **Bloom Filter**: 创建一个新的布隆过滤器，并将所有写入的key (`apple`, `cherry`, `date`, `elderberry`, `fig`, `grape`) 添加进去。
- **Footer**: 在文件末尾写入三个8字节的`long`：
  - `index_offset`: Index Block的起始位置。
  - `bloom_offset`: Bloom Filter的起始位置。
  - `magic_number`: `0x123456789ABCDEF0L`

至此，一个全新的、紧凑的 `1-15.sst` 文件在磁盘上构建完成。



#### 第5步：清理与收尾

- **目标**: 更新内存中的元数据，删除旧的物理文件。
- **过程**:
  1. **更新元数据 (`levels` map)**:
     - 从 `levels.get(0)` 中移除 `0-10`, `0-11`, `0-12`, `0-13`, `0-14` 的记录。L0变为空。
     - 从 `levels.get(1)` 中移除 `1-5` 的记录。
     - 将新生成的 `1-15.sst` 的 `SSTableReader` 添加到 `levels.get(1)` 中。
  2. **关闭文件句柄**: 调用所有旧的 `SSTableReader` 的 `close()` 方法，释放文件句柄。
  3. **删除物理文件**:
     - `Files.delete(".../0-10.sst")`
     - `Files.delete(".../0-11.sst")`
     - ...
     - `Files.delete(".../0-14.sst")`
     - `Files.delete(".../1-5.sst")`
  4. **释放写锁**: `metadataLock.writeLock().unlock()`。系统恢复正常读写服务。

合并完成。系统状态现在更健康：L0层变空了，L1层的数据更加紧凑，过期和已删除的数据被彻底清除，查询效率得到提升。