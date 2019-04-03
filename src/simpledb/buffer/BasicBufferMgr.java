package simpledb.buffer;

// CS4432-Project1: Importing to utilize linkedlists and hashtables
import simpledb.file.*;
import java.util.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   // CS4432-Project1: freeFrames - Holds the indices which we know to be free within the bufferpool
   //                  blockFrame - Hashtable which will store the block number as a key, and the value
   //                               will be the index within the bufferpool where the block can be found
   private Buffer[] bufferpool;
   private int numAvailable;
   private LinkedList<Integer> freeFrames;
   private Hashtable<Block, Integer> blockFrame;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      // CS4432-Project1: Keep track of the free frames by assigning
      freeFrames = new LinkedList<Integer>();

      // CS4432-Project1: Modified to add the indices to the LinkedList of freeFrames
      for (int i = 0; i < numbuffs; i++) {
         bufferpool[i] = new Buffer(i);
         freeFrames.add(i);
      }
      // CS4432-Project1: Initialize Hashtable, which size of numbuffs
      blockFrame = new Hashtable<Block, Integer>(numbuffs);

   }
   // CS4432-Project1: toString for task 2.5
   public String toString() {
      String poolStatus = "";
      for(Buffer buffer : bufferpool) {
         poolStatus += buffer.toString() + "\n";
      }
      return poolStatus;
   }

   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         // CS4432-Project1: Whenever adding buffer, we must add it to the hashtable as well, a reference
         blockFrame.put(blk, buff.getIndex());
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      // CS4432-Project1: Add the reference
      blockFrame.put(buff.block(), buff.getIndex());
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         // CS4432-Project1: When unpinning a buffer, add to the list of free frames by its original index
         freeFrames.add(buff.getIndex());
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
      if (blockFrame.get(blk) != null) {
         return bufferpool[blockFrame.get(blk)];
      } else {
         return null;
      }
   }

   // CS4432-Project1: Redefined method to obtain the first free buffer frame more efficiently.
   // Get the first element in the newly created list, if there is one.
   // If true, then return the buffer from the bufferpool with the found index from the linked list, else null
   private Buffer chooseUnpinnedBuffer() {
      Integer freeIndex = freeFrames.getFirst();
      freeFrames.removeFirst();

      // CS4432-Project1: If the list of freeFrames is returning null, we must decide how to replace the buffer
      if (numAvailable == 0) {
         freeIndex = null;
      }
      if (freeIndex != null) {
         // CS4432-Project1: Storing the lastTimeUsed to LRU
         Date lastUsed = bufferpool[0].getLastTimeUsed();
         int lastUsedIndex = 0;
         for(int i = 0; i < bufferpool.length; i++) {
            // CS4432-Project1: Compare the buffer to the other potential options. Make sure not pinned, and then check
            //  against the currently maintained lastTimeUsed for LRU implementation
            if(!bufferpool[i].isPinned() && bufferpool[i].getLastTimeUsed().compareTo(lastUsed) < 0) {
               lastUsed = bufferpool[i].getLastTimeUsed();
               lastUsedIndex = i;
            }
         }
         Buffer buffer = bufferpool[lastUsedIndex];

         if(buffer.block() != null) {
            blockFrame.remove(buffer.block());
         }
         return buffer;
//         return bufferpool[lastUsedIndex];
      }
      return null;
   }
}
