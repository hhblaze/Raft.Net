using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raft.Utils;

namespace Raft.Transport
{
    /// <summary>
    /// SPROT1
    /// Protocol structure:
    /// {0;0} {0;0}{0;0;0;0}{....} {0;0}{0;0;0;0}{....} {0;0}{0;0;0;0}{....} {0;0}{0;0;0;0}{....} 
    /// on connect comes 2 byte device identificator; codec - 2 bytes; codec length - 4 bytes; codec data;  codec; codec length; data; ...
    /// 
    /// restricted device Id starts:
    /// from 0x16 (22) SSL
    
    /// </summary>
    internal class cSprot1Parser
    {

        /////////////////////////////////////////////////////  FOR EXTERNAL SETUP


        /// <summary>
        /// Normally must be UInt
        /// </summary>
        private int _maxPayLoad = 2000000;

        /// <summary>
        /// Restriction. Protocol supports 4 bytes data transfer inside of codec, but we can add extra restriction on maximum size inside of codec.
        /// Inside of tcpServer there is also receive buffer size.
        /// Default 2000000 bytes.
        /// </summary>
        public int MaxPayLoad
        {
            get
            {
                return this._maxPayLoad;
            }
            set
            {
                if (value > 2000000000)
                    _maxPayLoad = 2000000000;
                else
                    _maxPayLoad = value;
            }
        }

        /// <summary>
        /// It's a queue for incoming data
        /// </summary>
        public SQueue<byte[]> MessageQueue = new SQueue<byte[]>();

        /// <summary>
        /// In case if MaxPayLoad is overriched this will be called
        /// </summary>
        public Action DestroySelf = null;
        /// <summary>
        /// Here you will receive parsed SPROT1 codecs, Method Will be called in Async Way, but in one thread. So streaming is possible.
        /// </summary>
        public Action<int, byte[]> packetParser = null;

        /// <summary>
        /// if this delegate is not null, parser after dequeuing procedure begins async invoke of the function and then goes on with parsing
        /// </summary>
        public Action<byte[]> PublishDequeued = null;

        /// <summary>
        /// If device establishes a connection, this must be true, for the other device which answers it must be false, default true (as for the server)
        /// </summary>
        public bool DeviceShouldSendAuthorisationBytesBeforeProceedCodec = true;
        //////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// In one of versions we wanted to enhance Sprot1 with message id, like this
        /// {0;0} -auth {0;0}-codec {0;0;0;0} - length of payload {0;0} - messageId {0;0;...;0} - payload
        /// In this case message Id is Extension, so to th length of payload will be added ProtocolExtensionLengthV1 (2 in our case) and the whole messageId+payload will be returned
        /// </summary>
        public int ProtocolExtensionLengthV1 = 0;

        /// <summary>
        /// For the server side implementer, can be interesting to get inside of the parser first authentication bytes,
        /// it will be done if true second parameter for Packet Parser will be null in this case. Default is false.
        /// </summary>
        public bool ToSendToParserAuthenticationBytes = false;

        /// <summary>
        /// By default Little Endian is used (First comes lower byte 0x01 in the end highest 0x08 = 2049)
        /// </summary>
        public bool UseBigEndian = false;


        private bool newPacket = true;
        /// <summary>
        /// We can use maximum size of 2,147,483,647 only int not uint because we use fast substrings of bytes which can be used only with int parameter
        /// </summary>
        private int newPacketSize = 0;

        private object lockObjPacketAnalizatorIsSleeping = new object();
        private bool packetAnalizatorIsSleeping = true;

        byte[] package = null;
        byte[] workingArray = null;
        int workingArrayFilled = 0;
        int codec = 0;
        //int cutOff = 0;

        /// <summary>
        /// Device Id
        /// </summary>
        int dId = -1;

        /// <summary>
        /// For debugging purposes. Default is null
        /// </summary>
        public string MyName { get; set; }


        /// <summary>
        /// Reinitialize Sprot instance after disconnection. In case if you don't want to create new instance for sure.
        /// Sets all technical variables back. Setting variables leaves in tact
        /// </summary>
        public void ReInitSprot()
        {
            newPacket = true;
            newPacketSize = 0;
            package = null;
            dId = -1;
            codec = 0;
            //cutOff = 0;
            packetAnalizatorIsSleeping = true;
            workingArray = null;
            workingArrayFilled = 0;
        }


        ///// <summary>
        ///// Main function for supplying data to Sprot Parser
        ///// </summary>
        ///// <param name="data"></param>
        //public void __DataArrived(byte[] data)
        //{
        //    this.MessageQueue.Enqueue(data);
        //    this.PacketAnalizator(false);
        //}


        /// <summary>
        /// Returns ready for sending data in BigEndian
        /// </summary>
        /// <param name="codec"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public static byte[] GetSprot1Codec(byte[] codec, byte[] data)
        {
            //return codec.Concat((data ?? new byte[] { }).Length.To_4_bytes_array_BigEndian()).Concat(data);
            return codec.Concat(((uint)(data ?? new byte[] { }).Length).To_4_bytes_array_BigEndian()).Concat(data);
        }

        /// <summary>
        /// Returns ready for sending data in different formats Big or LittleEndian
        /// </summary>
        /// <param name="codec"></param>
        /// <param name="data"></param>
        /// <param name="BigEndian"></param>
        /// <returns></returns>
        public static byte[] GetSprot1Codec(byte[] codec, byte[] data, bool BigEndian)
        {
            if (!BigEndian)
            {
                //return codec.Concat((data ?? new byte[] { }).Length.To_4_bytes_array_LittleEndian()).Concat(data);
                return codec.Concat(((uint)(data ?? new byte[] { }).Length).To_4_bytes_array_LittleEndian()).Concat(data);
            }
            else
            {
                //return codec.Concat((data ?? new byte[] { }).Length.To_4_bytes_array_BigEndian()).Concat(data);
                return codec.Concat(((uint)(data ?? new byte[] { }).Length).To_4_bytes_array_BigEndian()).Concat(data);
            }
        }


        #region "Spreading Ready Package in Parallel One thread."

        bool threadIsStarted = false;
        object lock_threadIsStarted = new object();

        /// <summary>
        /// Ready Package For Sending Out
        /// </summary>
        private class RPE
        {
            public int Codec { get; set; }
            public byte[] Data { get; set; }
        }

        /// <summary>
        /// Queue for storing Ready Packages
        /// </summary>
        SQueue<RPE> qrpe = new SQueue<RPE>();

        private void SpreadReadyPackage(int codec, byte[] data)
        {
            qrpe.Enqueue(new RPE()
            {
                Codec = codec,
                Data = data
            });

            lock (lock_threadIsStarted)
            {
                if (threadIsStarted)
                    return;

                threadIsStarted = true;
            }

            Action spreadAction = () =>
            {
                RPE rpe = null;

                while (true)
                {
                    lock (lock_threadIsStarted)
                    {
                        rpe = qrpe.Dequeue();
                        if (rpe == null)
                        {
                            threadIsStarted = false;
                            return;
                        }
                    }

                    this.packetParser(rpe.Codec, rpe.Data);
                }
            };

            
            Task.Run(spreadAction);

        }
        #endregion




        public void PacketAnalizator(bool recursiveSelfCall)
        {
            //decalring finalization function, 
            //result of function shows if we shoud stay in procedure or go out
            Func<bool> aFinalization = () =>
            {
                bool leftInQueue = false;

                //finalization
                lock (lockObjPacketAnalizatorIsSleeping)
                {
                    if (MessageQueue.Count > 0)
                        leftInQueue = true;
                    else
                    {
                        this.packetAnalizatorIsSleeping = true;
                        return true;
                    }
                }
                if (leftInQueue)
                {
                    this.PacketAnalizator(true);
                    return true;
                }

                return false;
            };

            //Sending from packet analizator to parser
            Action<int, byte[]> aParsePackage = (codec, data) =>
            {
                if (this.packetParser != null)
                {
                    //System.Threading.ThreadPool.QueueUserWorkItem(r =>
                    //{
                    //    this.packetParser(codec, data);
                    //});


                    //to avoid sprot packages "sending order" mismatch and to achieve current thread release, who makes SPROT computation, we use SpreadReadyPackage function.
                    //It guaranties that packetParser will receive in one thread correctly sequenced SPROT packages, what gives us opprtunity of data streaming.

                    this.SpreadReadyPackage(codec, data);

                }
            };




            if (!recursiveSelfCall)
            {
                lock (lockObjPacketAnalizatorIsSleeping)
                {
                    if (!this.packetAnalizatorIsSleeping)
                        return;

                    this.packetAnalizatorIsSleeping = false;
                }
            }

            bool toExit = false;

            byte[] dequeued = null;



            if (MessageQueue.Count > 0)
            {
                dequeued = MessageQueue.Dequeue();



                //special hack for those who wants to get extra full byte incoming visible. First will be called Action, then it will be EndInvoked, then we could call Callback. but in this case 
                //our Action makes everything, so no callBacks. More Async Examples also are in WSN_DistributedApplication.Host.cOperationExtensions.
                if (PublishDequeued != null)
                {
                    ///////////////////////////////////////////   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1   WARNING, take away SYNC?
                    //PublishDequeued(dequeued);
                    System.Threading.Tasks.Task.Run(() => PublishDequeued(dequeued));
                    //PublishDequeued.DoAsync<byte[]>(dequeued);

                    //PublishDequeued.BeginInvoke(dequeued, r =>
                    //{
                    //    ((Action<byte[]>)r.AsyncState).EndInvoke(r);
                    //}, PublishDequeued);
                }

                if (workingArray == null)
                {
                    this.package = this.package.Concat(dequeued);
                }
                else
                {
                    if (workingArrayFilled + dequeued.Length <= workingArray.Length)
                    {
                        workingArray.CopyInside(workingArrayFilled, dequeued, 0, dequeued.Length);
                        workingArrayFilled += dequeued.Length;

                    }
                    else
                    {
                        workingArray.CopyInside(workingArrayFilled, dequeued, 0, workingArray.Length - workingArrayFilled);
                        this.package = this.package.Concat(dequeued.Substring(workingArray.Length - workingArrayFilled));
                        workingArrayFilled += workingArray.Length - workingArrayFilled;
                    }
                }

                //normally this package can't be null and length must be always more then 1 here regulated by tcpServer

                if ((this.package != null && this.package.Length > this._maxPayLoad))
                {
                    //if (this.DestroySelf != null)
                    //    this.DestroySelf();
                    this.DestroySelf?.Invoke();

                    return;
                }
            }
            else
            {
                if (this.package == null && this.workingArray == null)
                    toExit = true;
                else
                {
                    if (this.package == null || this.package.Length < 1)
                    {
                        if (this.workingArray == null || this.workingArray.Length < 1)
                        {
                            toExit = true;
                        }
                    }
                }
            }

            //security check
            if (toExit)
            {
                if (aFinalization.Invoke()) return;
            }


            //{0;0}{0;0}{0;0;0;0}{....}   
            //pId - 2 byte device authentificator; codec - 2 bytes; codec length - 4 bytes; data;  code; codec length; data...

            //package binder
            if (this.newPacket)
            {
                if (this.package.Length < 2)
                {
                    //not enough length go on collect
                    if (aFinalization.Invoke()) return;
                }
                else
                {
                    if (dId == -1 && DeviceShouldSendAuthorisationBytesBeforeProceedCodec)
                    {
                        //Console.WriteLine("h1");

                        if (UseBigEndian)
                            dId = this.package.Substring(0, 2).To_UInt16_BigEndian();
                        else
                            dId = this.package.Substring(0, 2).To_UInt16_LittleEndian();

                        if (ToSendToParserAuthenticationBytes)
                        {
                            //sending authentification codec to parser
                            aParsePackage(dId, null);
                        }

                        this.package = this.package.Substring(2, this.package.Length);

                        if (this.package == null)
                        {
                            if (aFinalization.Invoke()) return;
                        }
                    }
                }

                if (this.package.Length < 6)
                {
                    //not enough length go on collect
                    if (aFinalization.Invoke())
                        return;
                }

                //Console.WriteLine("> " + this.package.Substring(0, 2).ToBytesString(" ") + "   " + this.package.Substring(2, 4).ToBytesString(" "));

                //getting codec 2 bytes        
                if (UseBigEndian)
                {
                    codec = this.package.Substring(0, 2).To_UInt16_BigEndian();
                    this.newPacketSize = ((int)this.package.Substring(2, 4).To_UInt32_BigEndian()) + ProtocolExtensionLengthV1;

                }
                else
                {
                    codec = this.package.Substring(0, 2).To_UInt16_LittleEndian();
                    this.newPacketSize = ((int)this.package.Substring(2, 4).To_UInt32_LittleEndian()) + ProtocolExtensionLengthV1;
                }

                this.newPacket = false;

                if (this.newPacketSize > this._maxPayLoad || this.newPacketSize < 0)
                {
                    //if (this.DestroySelf != null)
                    //    this.DestroySelf();
                    this.DestroySelf?.Invoke();

                    return;
                }

                //this.cutOff = this.newPacketSize + 6;
                workingArray = new byte[this.newPacketSize];


                if (this.package.Length >= 6)
                {

                    if (this.package.Length - 6 < this.newPacketSize)
                    {
                        workingArray.CopyInside(0, this.package, 6, this.package.Length - 6);
                        workingArrayFilled = this.package.Length - 6;
                        this.package = null;
                    }
                    else
                    {
                        workingArray.CopyInside(0, this.package, 6, this.newPacketSize);
                        workingArrayFilled = this.newPacketSize;
                        this.package = this.package.Substring(this.newPacketSize + 6);
                    }
                }

            }


            //if (this.package.Length >= this.cutOff)
            if (workingArrayFilled >= this.newPacketSize)
            {
                //here no extra threads are allowed may be in packetParser itself after authorisation
                //this.packetParser(codec, this.package.Substring(6, this.newPacketSize));

                //sending codec to parser
                //aParsePackage(codec, this.package.Substring(6, this.newPacketSize));
                //this.package = this.package.Substring(cutOff, this.package.Length);

                aParsePackage(codec, workingArray.Substring(0, this.newPacketSize));
                workingArray = null;
                workingArrayFilled = 0;
                this.newPacket = true;

                if (MessageQueue.Count > 0)
                {
                    this.PacketAnalizator(true);
                    return;
                }
                else if (this.package != null)
                {
                    if (this.package.Length > 0)
                    {
                        //Console.WriteLine(">>> " + this.package.Substring(0, 2).ToBytesString(" ") + "   " + this.package.Substring(2, 4).ToBytesString(" "));
                        // Console.WriteLine(">>> ");
                        this.PacketAnalizator(true);
                        return;
                    }
                }

                if (aFinalization.Invoke()) return;
            }

            if (aFinalization.Invoke()) return;

        }//eo function

        
    } //End of class

    /// <summary>
    /// !!! Take from DBreeze
    /// </summary>
    internal static class BytesExtensions
    {
        public static void CopyInside(this byte[] destArray, int destOffset, byte[] srcArray, int srcOffset, int quantity)
        {
            Buffer.BlockCopy(srcArray, srcOffset, destArray, destOffset, quantity);
        }

        /// <summary>
        /// Fastest Method. Works only for int-dimesional arrays only. 
        /// When necessary to concat many arrays use ConcatMany
        /// </summary>
        /// <param name="ar1"></param>
        /// <param name="ar2"></param>
        /// <returns></returns>
        public static byte[] Concat(this byte[] ar1, byte[] ar2)
        {
            if (ar1 == null)
                ar1 = new byte[] { };
            if (ar2 == null)
                ar2 = new byte[] { };

            byte[] ret = null;

            ret = new byte[ar1.Length + ar2.Length];

            Buffer.BlockCopy(ar1, 0, ret, 0, ar1.Length);
            Buffer.BlockCopy(ar2, 0, ret, ar1.Length, ar2.Length);

            return ret;
        }

        /// <summary>
        /// From Int32 to 4 bytes array with BigEndian order (highest byte first, lowest last).        
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static byte[] To_4_bytes_array_BigEndian(this uint value)
        {

            //if (!BitConverter.IsLittleEndian)
            //{
            //    return BitConverter.GetBytes(value);
            //}
            //else
            //{
            //    return BitConverter.GetBytes(value).Reverse().ToArray();
            //}
            return new byte[]
            {
                (byte)(value >> 24),
                (byte)(value >> 16),
                (byte)(value >> 8),
                (byte) value
            };
        }

        /// <summary>
        /// From Int32 to 4 bytes array with LittleEndian order (lowest byte first, highest last).        
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static byte[] To_4_bytes_array_LittleEndian(this uint value)
        {

            //if (!BitConverter.IsLittleEndian)
            //{
            //    return BitConverter.GetBytes(value).Reverse().ToArray();

            //}
            //else
            //{
            //    return BitConverter.GetBytes(value);
            //}

            return new byte[]
            {
                (byte) value ,
                (byte)(value >> 8),
                (byte)(value >> 16),
                (byte)(value >> 24),
            };
        }

        public static byte[] Substring(this byte[] ar, int startIndex)
        {
            //if (ar == null)
            //    return null;

            //return substringByteArray(ar, startIndex, ar.Length);


            int length = ar.Length;

            if (ar == null)
                return null;

            if (ar.Length < 1)
                return ar;

            if (startIndex > ar.Length - 1)
                return null;

            if (startIndex + length > ar.Length)
            {
                //we make length till the end of array
                length = ar.Length - startIndex;
            }

            byte[] ret = new byte[length];


            Buffer.BlockCopy(ar, startIndex, ret, 0, length);

            return ret;
        }

        /// <summary>
        /// Substring int-dimensional byte arrays
        /// </summary>
        /// <param name="ar"></param>
        /// <param name="startIndex"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public static byte[] Substring(this byte[] ar, int startIndex, int length)
        {
            if (ar == null)
                return null;

            if (ar.Length <= 0)
                return ar;

            if (startIndex > ar.Length - 1)
                return null;

            if (startIndex + length > ar.Length)
            {
                //we make length till the end of array
                length = ar.Length - startIndex;
            }

            byte[] ret = new byte[length];


            Buffer.BlockCopy(ar, startIndex, ret, 0, length);

            return ret;
        }


        /// <summary>
        /// From 2 bytes array which is in BigEndian order (highest byte first, lowest last) makes ushort.
        /// If array not equal 2 bytes throws exception. (0 to 65,535)
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static ushort To_UInt16_BigEndian(this byte[] value)
        {
            //if (!BitConverter.IsLittleEndian)
            //{
            //    return BitConverter.ToUInt16(value, 0);
            //}
            //else
            //{
            //    return BitConverter.ToUInt16(value.Reverse().ToArray(), 0);
            //}
            return (ushort)(value[0] << 8 | value[1]);
        }


        /// <summary>
        /// From 2 bytes array which is in LittleEndian order (lowest byte first, highest last) makes ushort.
        /// If array not equal 2 bytes throws exception. (0 to 65,535)
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static ushort To_UInt16_LittleEndian(this byte[] value)
        {
            //if (!BitConverter.IsLittleEndian)
            //{
            //    return BitConverter.ToUInt16(value.Reverse().ToArray(), 0);
            //}
            //else
            //{
            //    return BitConverter.ToUInt16(value, 0);
            //}

            return (ushort)(value[1] << 8 | value[0]);
        }



        /// <summary>
        /// From 4 bytes array which is in BigEndian order (highest byte first, lowest last) makes uint.
        /// If array not equal 4 bytes throws exception. (0 to 4.294.967.295)
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static uint To_UInt32_BigEndian(this byte[] value)
        {
            //if (!BitConverter.IsLittleEndian)
            //{
            //    return BitConverter.ToInt32(value, 0);
            //}
            //else
            //{
            //    return BitConverter.ToInt32(value.Reverse().ToArray(), 0);
            //}

            return (uint)(value[0] << 24 | value[1] << 16 | value[2] << 8 | value[3]);
        }

        /// <summary>
        /// From 4 bytes array which is in LittleEndian order (lowest byte first, highest last) makes uint.
        /// If array not equal 4 bytes throws exception. (0 to 4.294.967.295)
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static uint To_UInt32_LittleEndian(this byte[] value)
        {
            //if (!BitConverter.IsLittleEndian)
            //{
            //    return BitConverter.ToInt32(value.Reverse().ToArray(), 0);
            //}
            //else
            //{
            //    return BitConverter.ToInt32(value, 0);
            //}

            return (uint)(value[3] << 24 | value[2] << 16 | value[1] << 8 | value[0]);
        }



    }//EOC
}
