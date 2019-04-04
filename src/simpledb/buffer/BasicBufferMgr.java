package simpledb.buffer;

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
      System.out.println("Creating the basic buffer manager with " + numbuffs + " buffers.");
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
//   synchronized Buffer pin(Block blk) {
//      Buffer buff = findExistingBuffer(blk);
//      if (buff == null) {
//         buff = chooseUnpinnedBuffer();
//         if (buff == null)
//            return null;
//         buff.assignToBlock(blk);
//      }
//      if (!buff.isPinned())
//         numAvailable--;
//      buff.pin();
//      return buff;
//   }
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         System.out.println("the block does not exist, finding an unpinned buffer");
         buff = chooseUnpinnedBuffer();
         if (buff == null) {
            System.out.println("Choose unpinned buffer returned null");
            return null;
         }
         buff.assignToBlock(blk);
         System.out.println("assigned block " + blk + " to buffer index " + buff.getIndex());
         // CS4432-Project1: Whenever adding buffer, we must add it to the hashtable as well, a reference
         blockFrame.put(blk, buff.getIndex());
      }
      if (!buff.isPinned())
         System.out.println("Buffer is not pinned! Updating numAvailable from " + numAvailable + " to " + (numAvailable - 1));
         numAvailable--;
      buff.pin();
      System.out.println("Pinned buff!");
      //System.out.println("Pinning block " + blk + " to index number " + buff.getIndex() + ".");
      System.out.println("Number of buffers available after pin: " + numAvailable);
      System.out.println("Current buffer: " + blockFrame);
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
      //System.out.println("Pinning block " + buff.block() + " to index number " + buff.getIndex() + ".");
      System.out.println("Number of buffers available after pinNew: " + numAvailable);
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
      //System.out.println("Unpinning buffer " + buff + " at index " + buff.getIndex() + ".");
      System.out.println("Number of available buffers after unpin: " + numAvailable + ".");
   }

   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }

   private Buffer findExistingBuffer(Block blk) {
      for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
   }

   private Buffer chooseUnpinnedBuffer() {
      //System.out.println("Trying to find a free space for buffer.");
      Integer freeSpace = freeFrames.getFirst();
      freeFrames.removeFirst();

      System.out.println("Free spaces in buffer: " + numAvailable);

      if(freeSpace == null) {
         if(numAvailable == 0) {
            System.out.println("No free space in buffer! Using LRU to remove buffer.");
            freeSpace = null;
         } else {
            int index = 0;
            int lruIndex = 0;
            Date lruDate = bufferpool[0].getLastTimeUsed();

            for(index = 0; index<bufferpool.length; index++) {
               if(!bufferpool[index].isPinned() && bufferpool[index].getLastTimeUsed().compareTo(lruDate) < 0) {
                  lruIndex = index;
               }
            }
            System.out.println("Least recently used buffer: " + bufferpool[lruIndex]);
            return bufferpool[lruIndex];
         }
      } else {
         Buffer buffer = bufferpool[freeSpace];

         if (buffer.block() != null) {
            blockFrame.remove(buffer.block());
         }
         return buffer;
      }
      return null;

   }
}
