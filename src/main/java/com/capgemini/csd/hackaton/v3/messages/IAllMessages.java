package com.capgemini.csd.hackaton.v3.messages;

import com.capgemini.csd.hackaton.v3.Messages;

public interface IAllMessages {

	void init(String dossier);

	void close();

	Messages getForWrite(long s);

	Messages getForRead(long s);

}
