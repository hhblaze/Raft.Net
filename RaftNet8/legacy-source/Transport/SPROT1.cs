/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Raft.Utils;
using DBreeze.Utils;

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
        /// In case if MaxPayLoad is overflowed this will be called
        /// </summary>
        public Action DestroySelf = null;
        /// <summary>
        /// Here will be receive parsed SPROT1 codecs
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
        /// In this case message Id is an Extension, where to the length of payload will be added ProtocolExtensionLengthV1 (2 in our case) and the complete messageId+payload will be returned
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
            return codec.ConcatMany(((uint)(data ?? new byte[] { }).Length).To_4_bytes_array_BigEndian(), data);
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
                return codec.ConcatMany(((uint)(data ?? new byte[] { }).Length).To_4_bytes_array_LittleEndian(), data);
            else
                return codec.ConcatMany(((uint)(data ?? new byte[] { }).Length).To_4_bytes_array_BigEndian(), data);
        }


        #region "Spreading Ready Package"

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
            //declaring finalization function, 
            //result of function shows either we should stay in procedure or go out
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
                    this.SpreadReadyPackage(codec, data);
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
                //our Action makes everything, so - no callBacks.
                if (PublishDequeued != null)
                    Task.Run(() => PublishDequeued(dequeued));

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

                //normally this package can't be null and length must be always more than 1
                if ((this.package != null && this.package.Length > this._maxPayLoad))
                {                   
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
            
            if (workingArrayFilled >= this.newPacketSize)
            {
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
                        this.PacketAnalizator(true);
                        return;
                    }
                }

                if (aFinalization.Invoke()) return;
            }

            if (aFinalization.Invoke()) return;

        }//eo function

        
    } //End of class

}
